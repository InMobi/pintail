package com.inmobi.messaging.consumer.databus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;

class LocalStreamReader extends StreamReader {

  private static final Log LOG = LogFactory.getLog(LocalStreamReader.class);

  private final String collector;
  private final Path localStreamDir;

  LocalStreamReader(PartitionId partitionId, 
      Cluster cluster, String streamName) {
    this.collector = partitionId.getCollector();

    // initialize cluster and its directories
    super.init(partitionId, cluster, streamName);
    this.localStreamDir = getLocalStreamDir(cluster, streamName);
    LOG.info("LocalStream File reader initialized with partitionId:" +
         partitionId + " local stream dir: " + localStreamDir );
  }

  void build(Date buildTimestamp) throws IOException {
    LOG.info("Building file list from timestamp:" + buildTimestamp);
    assert(buildTimestamp != null);
    files = new TreeMap<String, Path>();
    if (fs.exists(localStreamDir)) {
      buildList(buildTimestamp);
    }
    fileNameIterator = files.navigableKeySet().iterator();
  }

  private void buildList(Date buildTimestamp) throws IOException {
    Calendar current = Calendar.getInstance();
    Date now = current.getTime();
    current.setTime(buildTimestamp);
    while (current.getTime().before(now)) {
      Path dir = new Path(localStreamDir, dirFormat.format(current.getTime()));
      LOG.debug("Current dir :" + dir);
      if (fs.exists(dir)) {
        FileStatus[] fileStatuses = fs.listStatus(dir);
        if (fileStatuses == null || fileStatuses.length == 0) {
          LOG.info("No files in directory:" + dir);
          return;
        }
        for (FileStatus file : fileStatuses) {
          if (file.getPath().getName().startsWith(collector)) {
            LOG.debug("Adding Path:" + file.getPath());
            files.put(file.getPath().getName(), file.getPath());
          } else {
            LOG.debug("Ignoring file:" + file.getPath());
          }
        }
      }
      // go to next minute
      current.add(Calendar.MINUTE, 1);
    }
  }

  /**
   *  Comment out this method if partition reader should not read from start of
   *   stream
   *  if check point does not exist.
   */
  boolean initializeCurrentFile(PartitionCheckpoint checkpoint)
      throws Exception {
    resetCurrentFile();
    boolean ret = false;
    if (isLocalStreamFile(checkpoint.getFileName())) {
      ret = super.initializeCurrentFile(checkpoint);
      if (!ret) {
        LOG.info("Could not find checkpointed file. Reading from start of the" +
            " stream");
        return initFromStart();
      }
    } else {
      LOG.info("The file " + checkpoint.getFileName() + " is not a local stream file");
    }
    return ret;
  }

  protected Path getFileForCheckpoint(PartitionCheckpoint checkpoint)
      throws Exception {
    if (checkpoint.getFileName().startsWith(collector)) {
      return files.get(checkpoint.getFileName());
    }
    return null;
  }

  protected Path getFileForTimeStamp(Date timestamp)
      throws Exception {
    Map.Entry<String, Path> ceilingEntry = files.ceilingEntry(
        getLocalStreamFileName(collector, streamName, timestamp));
    if (ceilingEntry != null) {
      return ceilingEntry.getValue();
    } else {
      return null;
    }
  }

  String readLine() throws IOException {
    String line = null;
    if (inStream != null) {
      line = readLine(inStream, reader);
    }
    while (line == null) { // reached end of file
      if (!nextFile()) { // reached end of file list
        LOG.info("could not find next file. Rebuilding");
        build(getDateFromLocalStreamDir(localStreamDir,
            currentFile.getParent())); 
        if (!setIterator()) {
          LOG.info("Could not find current file in the stream");
          // set current file to next higher entry
          if (!setNextHigher()) {
            LOG.info("Could not find next higher entry for current file");
            return null;
          } else {
            // read line from next higher file
            LOG.info("Reading from " + currentFile + ". The next higher file" +
                " after rebuild");
            line = readLine(inStream, reader);
          }
        } else if (!nextFile()) { // reached end of stream
          LOG.info("Reached end of stream");
          return null;
        } else {
          LOG.info("Reading from " + currentFile + " after rebuild");
          line = readLine(inStream, reader); 
        }
      } else {
        // read line from next file
        LOG.info("Reading from next file " + currentFile);
        line = readLine(inStream, reader);
      }
    }
    return line;
  }

  static boolean isLocalStreamFile(String streamName, String collectorName, 
      String fileName) {
    return fileName.startsWith(collectorName + "-" + streamName);
  }

  public boolean setCurrentFile(String localStreamFileName, 
      long currentLineNum) throws IOException {
    if (files.containsKey(localStreamFileName)) {
      currentFile = files.get(localStreamFileName);
      setIterator();
      this.currentLineNum = currentLineNum;
      LOG.debug("Set current file:" + currentFile +
          "currentLineNum:" + currentLineNum);
      openCurrentFile(false);
      return true;
    } else {
      LOG.info("Did not find current file. Trying to set next higher");
      setNextHigher();
    }
    return false;
  }

  protected BufferedReader createReader(FSDataInputStream in)
      throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        new GZIPInputStream(in)));
    return reader;
  }

  boolean isLocalStreamFile(String fileName) {
    return isLocalStreamFile(streamName, collector, fileName);
  }


  static Date getDateFromLocalStreamFile(String fileName)
      throws Exception {
    return StreamReader.getDate(fileName, 2);
  }

  static Path getLocalStreamDir(Cluster cluster, String streamName) {
    return new Path(cluster.getLocalFinalDestDirRoot(), streamName);
  }

  static String getLocalStreamFileName(String collector, String collectorFile) {
    return collector + "-" + collectorFile + ".gz";  
  }

  static String getLocalStreamFileName(String collector, String streamName,
      Date date) {
    return collector + "-" + streamName + "-" +  fileFormat.format(date) 
        + ".gz";  
  }

  static Date getDateFromLocalStreamDir(Path localStreamDir, Path dir) {
    String pathStr = dir.toString();
    String dirString = pathStr.substring(localStreamDir.toString().length() + 1);
    try {
      return dirFormat.parse(dirString);
    } catch (ParseException e) {
      LOG.warn("Could not get date from directory passed", e);
    }
    return null;
  }

  static Date getBuildTimestamp(Date time, String streamName, 
      String collectorName, PartitionCheckpoint partitionCheckpoint) {
    String fileName = null;
    if (partitionCheckpoint != null) {
      fileName = partitionCheckpoint.getFileName();
      if (fileName != null && 
          !isLocalStreamFile(streamName, collectorName, fileName)) {
        fileName = LocalStreamReader.getLocalStreamFileName(
              collectorName, fileName);
      }
    }

    Date buildTimestamp = time;
    if (buildTimestamp == null) {
      try {
        return getDateFromLocalStreamFile(fileName);
      } catch (Exception e) {
        throw new RuntimeException("Invalid fileName:" + 
            fileName, e);
      }
    } else {
      return time;
    }
  }

}
