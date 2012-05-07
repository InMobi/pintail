package com.inmobi.messaging.consumer.databus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.Cluster;

class LocalStreamFileReader extends StreamFileReader {

  private static final Log LOG = LogFactory.getLog(LocalStreamFileReader.class);

  private final String collector;
  private final Path localStreamDir;
  private PathFilter pathFilter;

  LocalStreamFileReader(PartitionId partitionId, 
      Cluster cluster, String streamName) {
    this.collector = partitionId.getCollector();
    
    // initialize cluster and its directories
    this.localStreamDir = getLocalStreamDir(cluster, streamName);
    try {
      super.init(partitionId, cluster, streamName);
      pathFilter = new LocalStreamPathFilter(collector, fs);
      build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }    

    LOG.debug("LocalStream File reader initialized with partitionId:" + partitionId +
            " local stream dir: " + localStreamDir );
  }

  @Override
  protected void build() throws IOException {
    files.clear();
    if (fs.exists(localStreamDir)) {
      buildList(localStreamDir);
    }
    fileNameIterator = files.navigableKeySet().iterator();
  }
  
  private void buildList(Path dir) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(dir, pathFilter);
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.info("No files in directory:" + dir);
      return;
    }
    for (FileStatus file : fileStatuses) {
      if (file.isDir()) {
        buildList(file.getPath());
      } else {
        LOG.debug("adding " + file.getPath().getName() + " Path:" + file.getPath());
        files.put(file.getPath().getName(), file.getPath());
      }
    }
  }

  boolean initializeCurrentFile(PartitionCheckpoint checkpoint) throws Exception {
    resetCurrentFile();
    boolean ret = false;
    if (isLocalStreamFile(checkpoint.getFileName())) {
      ret = super.initializeCurrentFile(checkpoint);
      if (!ret) {
        LOG.info("Could not find checkpointed file. Reading from start of the stream");
        return initFromStart();
      }
    }
    return ret;
  }

  protected Path getFileForCheckpoint(PartitionCheckpoint checkpoint) throws Exception {
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
    String line = readLine(inStream, reader);
    while (line == null) { // reached end of file
      if (!nextFile()) { // reached end of file list
        build(); // rebuild file list
        if (!setIterator()) {
          LOG.warn("Could not find current file in the stream");
          // set current file to next higher entry
          if (!setNextHigher()) {
            LOG.warn("Could not find next higher entry for current file");
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
        LOG.info("Reading from " + currentFile);
        line = readLine(inStream, reader);
      }
    }
    return line;
  }

  boolean isLocalStreamFile(String fileName) {
    return fileName.startsWith(collector + "-" + streamName);
  }

  public boolean setCurrentFile(String localStreamFileName, 
      long currentLineNum) throws IOException {
    if (files.containsKey(localStreamFileName)) {
      currentFile = files.get(localStreamFileName);
      setIterator();
      this.currentLineNum = currentLineNum;
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

  final static class LocalStreamPathFilter implements PathFilter {
    String collector;
    FileSystem fs;
    
    LocalStreamPathFilter(String collector, FileSystem fs) {
      this.collector=collector;
      this.fs = fs;
    }

    @Override
    public boolean accept(Path p) {
      try {
        if (fs.getFileStatus(p).isDir()) {
          return true;
        }
      } catch (IOException ioe) {
        return false;
      }
      if (p.getName().startsWith(collector)) {
        return true;
      }
      return false;
    }
  }

  static Path getLocalStreamDir(Cluster cluster, String streamName) {
    return new Path(cluster.getLocalFinalDestDirRoot(), streamName);
  }
  
  static Date getDateFromFile(String fileName) throws Exception {
    return getDate(fileName, 2);
  }  

  static Path getDateDir(Cluster cluster, String streamName,  Date date)
      throws Exception{
    return new Path(cluster.getLocalDestDir(streamName, date));

  }
  
  static Path getDateDir(Cluster cluster, String streamName, String fileName)
      throws Exception {    
    Date date = getDateFromFile(fileName);
    return getDateDir(cluster, streamName, date);
  }

  static String getLocalStreamFileName(String collector, String collectorFile) {
    return collector + "-" + collectorFile + ".gz";  
  }

  static String getLocalStreamFileName(String collector, String streamName,
      Date date) {
    return collector + "-" + streamName + "-" +  dateFormat.format(date) + ".gz";  
  }

}
