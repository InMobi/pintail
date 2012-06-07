package com.inmobi.messaging.consumer.databus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

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
  private boolean noNewFiles = false; // this is purely for tests
  protected long currentOffset = 0;
  private boolean sameStream = false;

  CollectorStreamReader(PartitionId partitionId,
      Cluster cluster, String streamName, long waitTimeForFlush) {
    this(partitionId, cluster, streamName, waitTimeForFlush, false);
  }

  CollectorStreamReader(PartitionId partitionId,
      Cluster cluster, String streamName, long waitTimeForFlush,
      boolean noNewFiles) {
    Path streamDir = new Path(cluster.getDataDir(), streamName);
    this.collectorDir = new Path(streamDir, partitionId.getCollector());
    this.waitTimeForFlush = waitTimeForFlush;
    this.noNewFiles = noNewFiles;
    super.init(partitionId, cluster, streamName);
    pathFilter = new ScribePathFilter();
    LOG.info("Collector reader initialized with partitionId:" + partitionId +
        " streamDir:" + streamDir + 
        " collectorDir:" + collectorDir +
        " waitTimeForFlush:" + waitTimeForFlush);
  }

  protected void initCurrentFile() {
    super.initCurrentFile();
    sameStream = false;
  }

  void build() throws IOException {
    files = new TreeMap<String, Path>();
    LOG.info("Building file list");
    if (fs.exists(collectorDir)) {
      FileStatus[] fileStatuses = fs.listStatus(collectorDir, pathFilter);
      if (fileStatuses == null || fileStatuses.length == 0) {
        LOG.info("No files in directory:" + collectorDir);
        return;
      }
      for (FileStatus file : fileStatuses) {
        LOG.debug("Adding Path:" + file.getPath());
        files.put(file.getPath().getName(), file.getPath());
      }
    }
    fileNameIterator = files.navigableKeySet().iterator();
  }

  @Override
  protected Path getFileForCheckpoint(PartitionCheckpoint checkpoint)
      throws Exception {
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

  protected void resetCurrentFileSettings() {
    super.resetCurrentFileSettings();
    currentOffset = 0;
  }

  protected void skipOldData(FSDataInputStream in, BufferedReader reader)
      throws IOException {
    if (sameStream) {
      seekToOffset(in, reader);
      LOG.info("Seek to offset:" + currentOffset);
    } else {
      skipLines(in, reader, currentLineNum);
      sameStream = true;
    }
  }

  /**
   * Skip the number of lines passed.
   * 
   * @return the actual number of lines skipped.
   */
  private void seekToOffset(FSDataInputStream in, BufferedReader reader) 
      throws IOException {
    in.seek(currentOffset);
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
    String line = null;
    if (inStream != null) {
      line = readLine(inStream, reader);
    }
    while (line == null) { // reached end of file?
      build(); // rebuild file list
      if (!nextFile()) { //there is no next file
        if (noNewFiles) {
          // this boolean check is only for tests 
          return null;
        } else {
          if (!setIterator()) {
            LOG.info("Could not find current file in the stream");
            if (stillInCollectorStream()) {
              LOG.info("Staying in collector stream as earlier files still exist");
              if (!setNextHigher(currentFile.getName())) {
                LOG.info("Could not find next higher file.");
                waitForNextFileCreation();
                LOG.info("Reading from the next file after its created");
              } else {
                LOG.info("Reading from next higher file");
              }
            } else {
              LOG.info("Current file would have been moved to Local Stream");
              return null;
            }
          }
          waitForFlushAndReOpen();
          LOG.info("Reading from the same file after reopen");
        }
      } else {
        LOG.info("Reading from next file: " + currentFile);
      }
      line = readLine(inStream, reader);
    }
    currentOffset = inStream.getPos();
    return line;
  }

  void waitForFlushAndReOpen() throws Exception {
    Thread.sleep(waitTimeForFlush);
    openCurrentFile(false);    
  }

  void waitForNextFileCreation() throws Exception {
    while (!setNextHigher(currentFile.getName())) {
      LOG.info("Waiting for next new file creation");
      Thread.sleep(10);
      build();
    }
  }

  boolean stillInCollectorStream() throws IOException {
    Map.Entry<String, Path> firstEntry = getFirstEntry();
    if (firstEntry != null && 
        getFirstEntry().getKey().compareTo(currentFile.getName()) < 1) {
      return true;
    }
    return false;
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

  public static Date getDateFromCollectorFile(String fileName)
      throws Exception {
    return StreamReader.getDate(fileName, 1);
  }

  static String getCollectorFileName(String streamName, Date date) {
    return streamName + "-" +  fileFormat.get().format(date) + "_00000" ;  
  }
}
