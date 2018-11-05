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
import org.apache.commons.lang.SystemUtils;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.managedbeans.LoginBean;
import org.goobi.production.enums.PluginType;
import org.goobi.production.flow.jobs.HistoryAnalyserJob;
import org.goobi.production.plugin.PluginLoader;
import org.goobi.production.plugin.interfaces.IOpacPlugin;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.BeanHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.enums.StepEditType;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.unigoettingen.sub.search.opac.ConfigOpac;
import de.unigoettingen.sub.search.opac.ConfigOpacCatalogue;
import lombok.extern.log4j.Log4j;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.MetadataType;
import ugh.dl.Prefs;
import ugh.exceptions.MetadataTypeNotAllowedException;

@Log4j
public class QuartzHotfolderJob implements Job {
    private static final long FIVEMINUTES = 1000 * 60 * 5;

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        XMLConfiguration config = ConfigPlugins.getPluginConfig("intranda_admin_hotfolder_tib");
        String hotFolder = config.getString("hotfolder");
        Path hotFolderPath = Paths.get(hotFolder);
        if (!Files.exists(hotFolderPath) || !Files.isDirectory(hotFolderPath)) {
            log.error("Hotfolder does not exist or is no directory " + hotFolder);
            return;
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(hotFolderPath)) {
            for (Path dir : ds) {
                log.debug("working with folder " + dir.getFileName());
                if (!checkIfCopyingDone(dir)) {
                    continue;
                }
                Path lockFile = dir.resolve(".intranda_lock");
                if (Files.exists(lockFile)) {
                    continue;
                }
                try (OutputStream os = Files.newOutputStream(lockFile)) {
                }
                //the dir is done copying. read barcode from it and request catalogue
                Process p = createNewProcess(dir, config);
                if (p != null) {
                    // successfully created process. Copy files to new master-folder
                    Path masterPath = Paths.get(p.getImagesOrigDirectory(false));
                    Files.createDirectories(masterPath);
                    try (DirectoryStream<Path> dirDs = Files.newDirectoryStream(dir)) {
                        for (Path file : dirDs) {
                            try (OutputStream os = Files.newOutputStream(masterPath.resolve(file.getFileName()))) {
                                Files.copy(file, os);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error(e);
        } catch (InterruptedException | DAOException | SwapException e) {
            // never happens
        }
    }

    private Process createNewProcess(Path dir, XMLConfiguration config) {
        Process template = ProcessManager.getProcessById(config.getInt("templateId"));
        Prefs prefs = template.getRegelsatz().getPreferences();
        String barcode = dir.getFileName().toString();
        ConfigOpacCatalogue coc = ConfigOpac.getInstance().getCatalogueByName("TIB");
        IOpacPlugin importer = (IOpacPlugin) PluginLoader.getPluginByTitle(PluginType.Opac, "PICA");
        Process process = null;
        try {
            Fileformat ff = importer.search("12", barcode, coc, prefs);
            process = cloneTemplate(template);
            NeuenProzessAnlegen(process, template, ff, prefs);

        } catch (Exception e) {
            // TODO Write error file to hotfolder error-folder
            log.error(e);
            return null;
        }
        process.setTitel(barcode);
        return process;
    }

    private boolean checkIfCopyingDone(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return false;
        }
        Date now = new Date();
        FileTime dirAccessTime = Files.readAttributes(dir, BasicFileAttributes.class).lastModifiedTime();
        log.debug("now: " + now + " dirAccessTime: " + dirAccessTime);
        long smallestDifference = now.getTime() - dirAccessTime.toMillis();
        int fileCount = 0;
        try (DirectoryStream<Path> folderFiles = Files.newDirectoryStream(dir)) {
            for (Path file : folderFiles) {
                fileCount++;
                FileTime fileAccessTime = Files.readAttributes(file, BasicFileAttributes.class).lastModifiedTime();
                log.debug("now: " + now + " fileAccessTime: " + fileAccessTime);
                long diff = now.getTime() - fileAccessTime.toMillis();
                if (diff < smallestDifference) {
                    smallestDifference = diff;
                }
            }
        }
        return (FIVEMINUTES < smallestDifference) && fileCount > 0;
    }

    public void NeuenProzessAnlegen(Process process, Process template, Fileformat ff, Prefs prefs) throws Exception {

        for (Step step : process.getSchritteList()) {

            step.setBearbeitungszeitpunkt(process.getErstellungsdatum());
            step.setEditTypeEnum(StepEditType.AUTOMATIC);
            LoginBean loginForm = (LoginBean) Helper.getManagedBeanValue("#{LoginForm}");
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
