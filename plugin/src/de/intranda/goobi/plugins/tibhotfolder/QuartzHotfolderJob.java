package de.intranda.goobi.plugins.tibhotfolder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Date;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.goobi.beans.Process;
import org.goobi.beans.Processproperty;
import org.goobi.beans.Step;
import org.goobi.managedbeans.LoginBean;
import org.goobi.production.enums.PluginType;
import org.goobi.production.flow.jobs.AbstractGoobiJob;
import org.goobi.production.flow.jobs.HistoryAnalyserJob;
import org.goobi.production.plugin.PluginLoader;
import org.goobi.production.plugin.interfaces.IOpacPlugin;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.BeanHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.ScriptThreadWithoutHibernate;
import de.sub.goobi.helper.enums.StepEditType;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.sub.goobi.persistence.managers.PropertyManager;
import de.sub.goobi.persistence.managers.StepManager;
import de.unigoettingen.sub.search.opac.ConfigOpac;
import de.unigoettingen.sub.search.opac.ConfigOpacCatalogue;
import lombok.extern.log4j.Log4j;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.MetadataType;
import ugh.dl.Prefs;
import ugh.exceptions.MetadataTypeNotAllowedException;

@Log4j
public class QuartzHotfolderJob extends AbstractGoobiJob {
    private static final long FIVEMINUTES = 1000 * 60 * 1;

    @Override
    public String getJobName() {
        return "tib-hotfolder";
    }

    @Override
    public void execute() {
        XMLConfiguration config = ConfigPlugins.getPluginConfig("intranda_admin_hotfolder_tib");
        String hotFolder = config.getString("hotfolder");
        Path hotFolderPath = Paths.get(hotFolder);
        if (!Files.exists(hotFolderPath) || !Files.isDirectory(hotFolderPath)) {
            log.error("Hotfolder does not exist or is no directory " + hotFolder);
            return;
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(hotFolderPath)) {
            for (Path dir : ds) {
                Path lockFile = dir.resolve(".intranda_lock");
                if (Files.exists(lockFile)) {
                    log.debug("Skipping folder " + dir.getFileName() + " as it has a .lock file already.");
                    continue;
                }
                if (!checkIfCopyingDone(dir)) {
                    continue;
                }
                log.debug("Starting to import folder " + dir.getFileName());
                try (OutputStream os = Files.newOutputStream(lockFile)) {
                    log.debug("Lock file created: " + lockFile);
                }
                //the dir is done copying. read barcode from it and request catalogue
                Process p = createNewProcess(dir, config);
                if (p != null) {
                    // successfully created process. Copy files to new master-folder
                    Path masterPath = Paths.get(p.getImagesOrigDirectory(false));
                    Files.createDirectories(masterPath);
                    try (DirectoryStream<Path> dirDs = Files.newDirectoryStream(dir)) {
                        for (Path file : dirDs) {
                            String filename = file.getFileName().toString();
                            if ("thumbs.db".equalsIgnoreCase(filename) || ".ds_store".equalsIgnoreCase(filename) || ".intranda_lock".equalsIgnoreCase(
                                    filename)) {
                                continue;
                            }
                            try (OutputStream os = Files.newOutputStream(masterPath.resolve(file.getFileName()))) {
                                Files.copy(file, os);
                            }
                        }
                    }
                    //start first automatic step
                    List<Step> steps = StepManager.getStepsForProcess(p.getId());
                    for (Step s : steps) {
                        if (StepStatus.OPEN.equals(s.getBearbeitungsstatusEnum()) && s.isTypAutomatisch()) {
                            ScriptThreadWithoutHibernate myThread = new ScriptThreadWithoutHibernate(s);
                            myThread.start();
                        }
                    }
                    FileUtils.deleteQuietly(dir.toFile());
                } else {
                    log.error("The process for " + dir.getFileName() + " could not get created");
                }
            }
        } catch (IOException e) {
            log.error("IOException while creating a process ", e);
        } catch (DAOException | SwapException e) {
            log.error("Error occured while creating a process ", e);
        }
    }

    private Process createNewProcess(Path dir, XMLConfiguration config) {
        Process template = ProcessManager.getProcessById(config.getInt("templateId"));
        Prefs prefs = template.getRegelsatz().getPreferences();
        String folderName = dir.getFileName().toString();
        if (!folderName.contains("_")) {
            log.error("The folder name " + dir.getFileName()
                    + " does not contain any underscore to get the Scanner name from it. The name should be something like '89$140210016_ScannerABC'");
            return null;
        }

        String[] split = folderName.split("_");
        String barcode = split[0];
        String scanner = split[1];
        ConfigOpacCatalogue coc = ConfigOpac.getInstance().getCatalogueByName("TIB-TOC");
        IOpacPlugin importer = (IOpacPlugin) PluginLoader.getPluginByTitle(PluginType.Opac, "PICA");
        Process process = null;
        try {
            Fileformat ff = importer.search("8535", barcode, coc, prefs);
            process = cloneTemplate(template);
            process.setTitel(folderName.replace("$", "_"));
            NeuenProzessAnlegen(process, template, ff, prefs);
            Processproperty pp = new Processproperty();
            pp.setProzess(process);
            pp.setTitel("Scanner-Name");
            pp.setWert(scanner);
            PropertyManager.saveProcessProperty(pp);

        } catch (Exception e) {
            log.error("Exception happened while requesting the catalogue for " + dir.getFileName(), e);
            return null;
        }
        return process;
    }

    private boolean checkIfCopyingDone(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return false;
        }
        Date now = new Date();
        FileTime dirAccessTime = Files.readAttributes(dir, BasicFileAttributes.class).lastModifiedTime();
        //        log.debug("now: " + now + " dirAccessTime: " + dirAccessTime);
        long smallestDifference = now.getTime() - dirAccessTime.toMillis();
        int fileCount = 0;
        try (DirectoryStream<Path> folderFiles = Files.newDirectoryStream(dir)) {
            for (Path file : folderFiles) {
                fileCount++;
                FileTime fileAccessTime = Files.readAttributes(file, BasicFileAttributes.class).lastModifiedTime();
                //                log.debug("now: " + now + " fileAccessTime: " + fileAccessTime);
                long diff = now.getTime() - fileAccessTime.toMillis();
                if (diff < smallestDifference) {
                    smallestDifference = diff;
                }
            }
        }
        //        log.debug("Folder is old enough to start the import: " + (FIVEMINUTES < smallestDifference));
        //        log.debug("Number of files to import: " + fileCount);

        if ((FIVEMINUTES < smallestDifference) && fileCount > 0) {
            log.debug("Folder " + dir.getFileName() + " is old enough and contains files. Import can start.");
            return true;
        } else {
            log.debug("Folder " + dir.getFileName() + " is not old enough or does not contain files. Import skipped.");
            return false;
        }
    }

    public void NeuenProzessAnlegen(Process process, Process template, Fileformat ff, Prefs prefs) throws Exception {

        for (Step step : process.getSchritteList()) {

            step.setBearbeitungszeitpunkt(process.getErstellungsdatum());
            step.setEditTypeEnum(StepEditType.AUTOMATIC);
            LoginBean loginForm = Helper.getLoginBean();
            if (loginForm != null) {
                step.setBearbeitungsbenutzer(loginForm.getMyBenutzer());
            }

            if (step.getBearbeitungsstatusEnum() == StepStatus.DONE) {
                step.setBearbeitungsbeginn(process.getErstellungsdatum());

                Date myDate = new Date();
                step.setBearbeitungszeitpunkt(myDate);
                step.setBearbeitungsende(myDate);
            }

        }

        ProcessManager.saveProcess(process);

        Processproperty pp = new Processproperty();
        pp.setProzess(process);
        pp.setTitel("OLR ausführen");
        List<? extends Metadata> metaList =
                ff.getDigitalDocument().getLogicalDocStruct().getAllMetadataByType(prefs.getMetadataTypeByName("ConferenceIndicator"));
        String activate = "nein";
        if (metaList != null && !metaList.isEmpty()) {
            for (Metadata md : metaList) {
                if ("kn".equals(md.getValue()) || "Konferenzschrift".equals(md.getValue())) {
                    activate = "ja";
                }
            }
        }
        //        String activate = metaList == null || metaList.isEmpty() ? "nein" : "ja";
        pp.setWert(activate);
        PropertyManager.saveProcessProperty(pp);

        /*
         * -------------------------------- Imagepfad hinzufügen (evtl. vorhandene zunächst löschen) --------------------------------
         */
        try {
            MetadataType mdt = prefs.getMetadataTypeByName("pathimagefiles");
            List<? extends Metadata> alleImagepfade = ff.getDigitalDocument().getPhysicalDocStruct().getAllMetadataByType(mdt);
            if (alleImagepfade != null && alleImagepfade.size() > 0) {
                for (Metadata md : alleImagepfade) {
                    ff.getDigitalDocument().getPhysicalDocStruct().getAllMetadata().remove(md);
                }
            }
            Metadata newmd = new Metadata(mdt);
            if (SystemUtils.IS_OS_WINDOWS) {
                newmd.setValue("file:/" + process.getImagesDirectory() + process.getTitel().trim() + "_tif");
            } else {
                newmd.setValue("file://" + process.getImagesDirectory() + process.getTitel().trim() + "_tif");
            }
            ff.getDigitalDocument().getPhysicalDocStruct().addMetadata(newmd);

            /* Rdf-File schreiben */
            process.writeMetadataFile(ff);

        } catch (ugh.exceptions.DocStructHasNoTypeException | MetadataTypeNotAllowedException e) {
            log.error(e);
        }

        // Adding process to history
        HistoryAnalyserJob.updateHistoryForProzess(process);

        ProcessManager.saveProcess(process);

        process.readMetadataFile();

    }

    private Process cloneTemplate(Process template) {
        Process process = new Process();

        process.setIstTemplate(false);
        process.setInAuswahllisteAnzeigen(false);
        process.setProjekt(template.getProjekt());
        process.setRegelsatz(template.getRegelsatz());
        process.setDocket(template.getDocket());

        BeanHelper bHelper = new BeanHelper();
        bHelper.SchritteKopieren(template, process);
        bHelper.ScanvorlagenKopieren(template, process);
        bHelper.WerkstueckeKopieren(template, process);
        bHelper.EigenschaftenKopieren(template, process);

        return process;
    }

}
