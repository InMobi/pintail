package com.inmobi.messaging.consumer.util;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;

public class HadoopUtil {

  public static void setUpHadoopFiles(Path finalDir, Configuration conf,
      String[] files, Path[] finalFiles)
          throws Exception {
    FileSystem fs = finalDir.getFileSystem(conf);
    Path rootDir = finalDir.getParent();
    Path tmpDataDir = new Path(rootDir, "data");
    // setup data dirs
    if (files != null) {
      int i = 0;
      int j = 0;
      for (String file : files) {
        MessageUtil.createMessageSequenceFile(file, fs, tmpDataDir, i, conf);
        i += 100;
        Path movedPath = moveDataFile(fs, tmpDataDir, file, finalDir);
        finalFiles[j] = movedPath;
        j++;
      }
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
    cal.add(Calendar.MINUTE, -1);
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
      String[] files,
      Path[] finalFiles, Path finalDir)
          throws Exception {
    FileSystem fs = finalDir.getFileSystem(conf);
    
    Path rootDir = finalDir.getParent();
    fs.delete(rootDir, true);
    Path tmpDataDir = new Path(rootDir, "data");
    fs.mkdirs(tmpDataDir);
    
    setUpHadoopFiles(finalDir, conf, files, finalFiles);
  }

  private static Path moveDataFile(FileSystem fs, Path dataDir,
      String fileName, Path finalDir)
          throws Exception {
    Path targetFile = getTargetPath(fileName, finalDir);
    Path srcPath =  new Path(dataDir, fileName);
    fs.rename(srcPath, targetFile);
    return targetFile;
  }

  private static Path getTargetPath(String fileName, Path streamDirPrefix) 
      throws IOException {
    Path streamDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDirPrefix,
        TestUtil.getCommitDateForCollectorFile(fileName));
    return new Path(streamDir, fileName);
  }
}
