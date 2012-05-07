package com.inmobi.messaging.consumer.databus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.Cluster;

class CollectorStreamReader extends StreamReader {

  private static final Log LOG = LogFactory.getLog(CollectorStreamReader.class);

  private final Path collectorDir;
  private PathFilter pathFilter;
  private long waitTimeForFlush;

  CollectorStreamReader(PartitionId partitionId,
      Cluster cluster, String streamName, long waitTimeForFlush) {
    Path streamDir = new Path(cluster.getDataDir(), streamName);
    this.collectorDir = new Path(streamDir, partitionId.getCollector());
    this.waitTimeForFlush = waitTimeForFlush;
    try {
      super.init(partitionId, cluster, streamName);
      pathFilter = new ScribePathFilter();
      build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.debug("Collector reader initialized with partitionId:" + partitionId +
          " streamDir:" + streamDir + 
          " collectorDir:" + collectorDir +
          " waitTimeForFlush" + waitTimeForFlush);
  }

  @Override
  protected void build() throws IOException {
    files.clear();
    LOG.debug("Rebuilding file list");
    if (fs.exists(collectorDir)) {
      FileStatus[] fileStatuses = fs.listStatus(collectorDir, pathFilter);
      if (fileStatuses == null || fileStatuses.length == 0) {
        LOG.info("No files in directory:" + collectorDir);
        return;
      }
      for (FileStatus file : fileStatuses) {
        files.put(file.getPath().getName(), file.getPath());
      }
    }
    fileNameIterator = files.navigableKeySet().iterator();
  }
 
  @Override
  protected Path getFileForCheckpoint(PartitionCheckpoint checkpoint) throws Exception {
    if (isCollectorFile(checkpoint.getFileName())) {
      Path checkpointPath = new Path(collectorDir, checkpoint.getFileName());
      if (fs.exists(checkpointPath)) {
        return checkpointPath;
      }
    }
    return null;
  }

  @Override
  protected BufferedReader createReader(FSDataInputStream in)
      throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    return reader;
  }


  @Override
  protected Path getFileForTimeStamp(Date startTime)
      throws Exception {
    Map.Entry<String, Path> ceilingEntry = files.ceilingEntry(
        getCollectorFileName(streamName, timestamp));
      if (ceilingEntry != null) {
        return ceilingEntry.getValue();
      } else {
        return null;
      }
  }

  String readLine() throws Exception {
    String line = readLine(inStream, reader);
    while (line == null) { // reached end of file?
      // see if currentFile is current file is Scribe file
      if (isCurrentScribeFile()) {
        LOG.info("waiting for current file to be flushed");
        Thread.sleep(waitTimeForFlush);
        openCurrentFile(false);
        LOG.info("Reading from same file after reopen");
        line = readLine(inStream, reader);
      } else {
        build(); // rebuild file list
        if (!setIterator()) {
          LOG.warn("Could not find current file in the stream");
          return null;
        } else if (!nextFile()) { //there is no next file, reached end of stream
          LOG.info("Reached end of stream");
          return null;
        } else {
          LOG.info("Reading from next file: " + currentFile);
          line = readLine(inStream, reader); 
        }
      } 
    }
    return line;
  }


  boolean isCollectorFile(String fileName) {
    return fileName.startsWith(streamName);
  }

  final static class ScribePathFilter implements PathFilter {
    
    ScribePathFilter() {
    }

    @Override
    public boolean accept(Path p) {
      if (p.getName().endsWith("current")
          || p.getName().equals("scribe_stats")) {
        return false;
      }
      return true;
    }
  }

  private boolean isCurrentScribeFile() throws IOException {
    if (currentFile.getName().equals(getCurrentScribeFile())) {
      return true;
    } else {
      return false;
    }
  }
  private String getCurrentScribeFile() throws IOException {
    Path currentScribeFile = new Path(collectorDir, streamName + "_current");
    String currentFileName = null;
    if (fs.exists(currentScribeFile)) {
      FSDataInputStream in = fs.open(currentScribeFile);
      String line = new BufferedReader(new InputStreamReader(in)).readLine();
      if (line != null) {
        currentFileName = line.trim();
      }
      in.close();
    }
    return currentFileName;
  }

  static Date getDateFromFile(String fileName) throws Exception {
    return getDate(fileName, 1);
  }
  
  static Path getDateDir(Cluster cluster, String streamName, String fileName)
      throws Exception {    
    Date date = getDateFromFile(fileName);
    return LocalStreamReader.getDateDir(cluster, streamName, date);
  }

  static String getCollectorFileName(String streamName, Date date) {
    return streamName + "-" +  dateFormat.format(date) ;  
  }
  static String getCollectorFileName(String localStreamFile) {
    return localStreamFile.substring(localStreamFile.indexOf('-') + 1,
        localStreamFile.indexOf('.'));  
  }

  
}
