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

import org.apache.commons.configuration.XMLConfiguration;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import de.sub.goobi.config.ConfigPlugins;
import lombok.extern.log4j.Log4j;

@Log4j
public class QuartzHotfolderJob implements Job {
    private static final long FIVEMINUTES = 1000 * 60 * 5;

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        XMLConfiguration config = ConfigPlugins.getPluginConfig("intranda_admin_catalogue_poller");
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
                //TODO: the dir is done copying. read barcode from it and request catalogue
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error(e);
        }
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

}
