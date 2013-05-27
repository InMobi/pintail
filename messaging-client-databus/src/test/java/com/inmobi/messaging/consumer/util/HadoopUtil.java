package com.inmobi.messaging.consumer.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;

public class HadoopUtil {
  static final Log LOG = LogFactory.getLog(HadoopUtil.class);

  private static Date lastCommitTime;
  private static int increment = 1;
  public static String[] files = new String[12];
  static Date startCommitTime;
  private static String fileNamePrefix = "datafile";
  static {
    Calendar now = Calendar.getInstance();
    now.add(Calendar.MINUTE, - (files.length + 5));
    startCommitTime = now.getTime();
    LOG.debug("startCommitTime:" + startCommitTime);
    for (int i = 0; i < files.length; i++) {
      files[i] = fileNamePrefix + i;
    }
  }

  public static void incrementCommitTime() {
    increment++;
  }

  public static void setUpHadoopFiles(Path streamDirPrefix, Configuration conf,
      String[] files, String[] suffixDirs, Path[] finalFiles)
          throws Exception {
    FileSystem fs = streamDirPrefix.getFileSystem(conf);
    Path rootDir = streamDirPrefix.getParent();
    Path tmpDataDir = new Path(rootDir, "data");
    // setup data dirs
    if (files != null) {
      int i = 0;
      int j = 0;
      for (String file : files) {
        MessageUtil.createMessageSequenceFile(file, fs, tmpDataDir, i, conf);
        i += 100;
        Path srcPath =  new Path(tmpDataDir, file);
        Date commitTime = getCommitDateForFile(file);
        TestUtil.publishMissingPaths(fs, streamDirPrefix, lastCommitTime,
            commitTime);
        lastCommitTime = commitTime;
        Path targetDateDir = getTargetDateDir(streamDirPrefix, commitTime);
        List<Path> targetDirs = new ArrayList<Path>();
        if (suffixDirs != null) {
          for (String suffixDir : suffixDirs) {
            targetDirs.add(new Path(targetDateDir, suffixDir));
          }
        } else {
          targetDirs.add(targetDateDir);
        }
        for (Path targetDir : targetDirs) {
          fs.mkdirs(targetDir);
          Path targetPath = new Path(targetDir, file);
          fs.copyFromLocalFile(srcPath, targetPath);
          LOG.info("Copied " + srcPath + " to " + targetPath);
          if (finalFiles != null) {
            finalFiles[j] = targetPath;
            j++;
          }
          Thread.sleep(1000);
        }
        fs.delete(srcPath, true);
      }
      TestUtil.publishLastPath(fs, streamDirPrefix, lastCommitTime);
    }
  }

  public static HadoopStreamFile getOlderFile(Path streamDirPrefix,
      FileSystem fs, Path databusFile) throws IOException {
    FileStatus stat = fs.getFileStatus(databusFile);
    HadoopStreamFile hs = HadoopStreamFile.create(stat);
    Calendar cal = Calendar.getInstance();
    Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDirPrefix,
        hs.getParent());
    cal.setTime(date);
    cal.add(Calendar.HOUR_OF_DAY, -1);
    return new HadoopStreamFile(
        DatabusStreamWaitingReader.getMinuteDirPath(streamDirPrefix,
            cal.getTime()),
        "myfile",
        hs.getTimestamp() - 36000);
  }

  public static HadoopStreamFile getHigherFile(Path streamDirPrefix,
      FileSystem fs, Path databusFile) throws IOException {
    FileStatus stat = fs.getFileStatus(databusFile);
    HadoopStreamFile hs = HadoopStreamFile.create(stat);
    Calendar cal = Calendar.getInstance();
    Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDirPrefix,
        hs.getParent());
    cal.setTime(date);
    cal.add(Calendar.MINUTE, 1);
    return new HadoopStreamFile(
        DatabusStreamWaitingReader.getMinuteDirPath(streamDirPrefix,
            cal.getTime()), "myfile", hs.getTimestamp() + 36000);
  }

  public static void setupHadoopCluster(Configuration conf,
      String[] files, String[] suffixDirs,
      Path[] finalFiles, Path finalDir)
          throws Exception {
    FileSystem fs = finalDir.getFileSystem(conf);
    
    Path rootDir = finalDir.getParent();
    fs.delete(rootDir, true);
    Path tmpDataDir = new Path(rootDir, "data");
    fs.mkdirs(tmpDataDir);
    
    setUpHadoopFiles(finalDir, conf, files, suffixDirs, finalFiles);
  }

  private static Path getTargetDateDir(Path streamDirPrefix, Date commitTime)
      throws IOException {
    Path dateDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDirPrefix,
        commitTime);
    return dateDir;
  }

  static Date getCommitDateForFile(String fileName)
      throws IOException {
    Calendar cal = Calendar.getInstance();
    cal.setTime(startCommitTime);
    LOG.debug("index for " + fileName + ":" + getIndex(fileName));
    cal.add(Calendar.MINUTE, getIndex(fileName));
    cal.add(Calendar.MINUTE, increment);
    LOG.debug("Commit time for file:" + fileName + " is " + cal.getTime());
    return cal.getTime();
  }

  private static int getIndex(String fileName) {
    try {
      String indexStr = fileName.substring(fileNamePrefix.length());
      LOG.debug("indexStr:" + indexStr);
      if (indexStr != null && !indexStr.isEmpty()) {
        return Integer.parseInt(indexStr);
      }
    } catch (Exception e) {
      LOG.debug("Exception while getting file index for " + fileName, e);
    }
    return 1;
  }
}
