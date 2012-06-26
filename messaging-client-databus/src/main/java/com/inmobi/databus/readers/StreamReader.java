package com.inmobi.databus.readers;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;

public abstract class StreamReader<T extends StreamFile> {

  private static final Log LOG = LogFactory.getLog(StreamReader.class);

  protected String streamName;
  protected FileMap<T> fileMap;
  protected Date timestamp;
  protected PartitionCheckpoint checkpoint;
  protected Cluster cluster;
  protected PartitionId partitionId;
  protected Path currentFile;
  protected long currentLineNum = 0;
  protected FileSystem fs;
  protected volatile boolean closed = false;
  protected boolean noNewFiles = false; // this is purely for tests
  protected long waitTimeForCreate = 100;
  protected Path streamDir;

  protected StreamReader(PartitionId partitionId, Cluster cluster, 
      String streamName) throws IOException {
    this.streamName = streamName;
    this.cluster = cluster;
    this.partitionId = partitionId;
    this.fs = FileSystem.get(cluster.getHadoopConf());
    this.fileMap = createFileMap();
    this.streamDir = getStreamDir(cluster, streamName);
  }

  protected FSDataInputStream inStream;
  protected BufferedReader reader;

  public void openStream() throws IOException {
    openCurrentFile(false);
  }

  public void closeStream() throws IOException {
    closeCurrentFile();    
  }

  public void close() throws IOException {
    closeStream();
    closed = true;
  }

  protected abstract FileMap<T> createFileMap() throws IOException;

  protected abstract Path getStreamDir(Cluster cluster, String streamName);

  public void build() throws IOException {
    fileMap.build();
  }

  protected void openCurrentFile(boolean next) throws IOException {
    closeCurrentFile();
    if (next) {
      resetCurrentFileSettings();
    } 
    LOG.info("Opening file:" + currentFile);
    LOG.debug("NumLinesTobeSkipped when opening:" + currentLineNum);
    if (fs.exists(currentFile)) {
      inStream = fs.open(currentFile);
      reader = getReader(inStream);
    } else {
      LOG.debug("CurrentFile:" + currentFile + " does not exist");
    }
  }

  private synchronized void closeReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  private synchronized void closeCurrentFile() throws IOException {
    closeReader();
    if (inStream != null) {
      inStream.close();
      inStream = null;
    }
  }

  protected boolean setIterator() {
    return fileMap.setIterator(currentFile);
  }

  protected void initCurrentFile() {
    currentFile = null;
    resetCurrentFile();    
  }

  public boolean initializeCurrentFile(Date timestamp) throws IOException {
    initCurrentFile();
    this.timestamp = timestamp;
    currentFile = fileMap.getCeilingValue(
        getStreamFileName(streamName, timestamp));

    if (currentFile != null) {
      setIterator();
      LOG.debug("CurrentFile:" + currentFile + " currentLineNum:"+ 
          currentLineNum);
    }
    return currentFile != null;
  }

  public boolean initializeCurrentFile(PartitionCheckpoint checkpoint)
      throws IOException {
    initCurrentFile();
    if (!isStreamFile(checkpoint.getFileName())) {
      LOG.info("The file " + checkpoint.getFileName() + " is not a " +
          "stream file");
      return false;
    }
    this.checkpoint = checkpoint;
    LOG.debug("checkpoint:" + checkpoint);
    currentFile = fileMap.getValue(checkpoint.getFileName());
    if (currentFile != null) {
      currentLineNum = checkpoint.getLineNum();
      LOG.debug("CurrentFile:" + currentFile + " currentLineNum:" + 
          currentLineNum);
      setIterator();
    } 
    return currentFile != null;
  }

  public boolean initFromStart() throws IOException {
    initCurrentFile();
    currentFile = fileMap.getFirstFile();

    if (currentFile != null) {
      LOG.debug("CurrentFile:" + currentFile + " currentLineNum:" + 
          currentLineNum);
      setIterator();
    }
    return currentFile != null;
  }

  public abstract boolean isStreamFile(String fileName);

  protected void resetCurrentFile() {
    currentFile = null;
    resetCurrentFileSettings();
  }

  public boolean isEmpty() {
    return fileMap.isEmpty();
  }

  protected boolean setNextHigher(String currentFileName) throws IOException {
    LOG.debug("finding next higher for " + currentFileName);
    Path nextHigherFile  = fileMap.getHigherValue(currentFileName);
    if (nextHigherFile != null) {
      currentFile = nextHigherFile;
      LOG.debug("Next higher entry:" + currentFile);
      setIterator();
      openCurrentFile(true);
      return true;
    }
    return false;
  }

  public Path getCurrentFile() {
    return currentFile;
  }

  public long getCurrentLineNum() {
    return currentLineNum;
  }

  protected abstract String getStreamFileName(String streamName, Date timestamp);

  protected abstract BufferedReader createReader(FSDataInputStream in)
      throws IOException;

  /** 
   * Returns null when reached end of stream 
   */
  public abstract String readLine() throws IOException, InterruptedException;

  /**
   * Skip the number of lines passed.
   * 
   * @return the actual number of lines skipped.
   */
  protected long skipLines(FSDataInputStream in, BufferedReader reader, 
      long numLines) 
          throws IOException {
    long lineNum = 0;
    while (lineNum != numLines) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      lineNum++;
    }
    LOG.info("Skipped " + lineNum + " lines");
    if (lineNum != numLines) {
      LOG.warn("Skipped wrong number of lines");
    }
    return lineNum;
  }

  protected String readLine(FSDataInputStream in, BufferedReader reader)
      throws IOException {
    if (reader != null) {
      String line = reader.readLine();
      if (line != null) {
        currentLineNum++;
      }
      return line;
    }
    return null;
  }

  protected BufferedReader getReader(FSDataInputStream in) throws IOException {
    BufferedReader reader = createReader(in);
    skipOldData(in, reader);
    return reader;
  }


  protected abstract void skipOldData(FSDataInputStream in,
      BufferedReader reader) throws IOException;

  protected void resetCurrentFileSettings() {
    currentLineNum = 0;
  }

  protected boolean nextFile() throws IOException {
    LOG.debug("In next file");
    if (!setIterator()) {
      LOG.info("could not set iterator for currentfile");
      return false;
    }
    Path nextFile = fileMap.getNext();
    if (nextFile != null) {
      currentFile = nextFile;
      openCurrentFile(true);
      return true;
    }
    return false;
  }

  public boolean setCurrentFile(String streamFileName, 
      long currentLineNum) throws IOException {
    if (fileMap.containsFile(streamFileName)) {
      currentFile = fileMap.getValue(streamFileName);
      setIterator();
      this.currentLineNum = currentLineNum;
      LOG.debug("Set current file:" + currentFile +
          "currentLineNum:" + currentLineNum);
      openCurrentFile(false);
      return true;
    } else {
      LOG.info("Did not find current file." + streamFileName +
          " Trying to set next higher");
      if (!setNextHigher(streamFileName)) {
        return false;
      } else {
        return true;
      }
    }
  }

  public void startFromNextHigher(String collectorFileName) 
      throws IOException, InterruptedException {
    if (!setNextHigher(collectorFileName)) {
      if (noNewFiles) {
        // this boolean check is only for tests 
        return;
      }
      waitForNextFileCreation(collectorFileName);
    }
  }

  public void startFromTimestmp(Date timestamp) throws IOException,
      InterruptedException {
    if (!initializeCurrentFile(timestamp)) {
      if (noNewFiles) {
        // this boolean check is only for tests 
        return;
      }
      waitForNextFileCreation(timestamp);
    }
  }

  public void startFromBegining() throws IOException, InterruptedException {
    if (!initFromStart()) {
      if (noNewFiles) {
        // this boolean check is only for tests 
        return;
      }
      waitForNextFileCreation();
    }
  }

  private void waitForNextFileCreation() throws IOException,
      InterruptedException {
    while (!closed && !initFromStart()) {
      LOG.info("Waiting for next file creation");
      Thread.sleep(waitTimeForCreate);
      build();
    }
  }

  private void waitForNextFileCreation(Date timestamp) throws IOException,
      InterruptedException {
    while (!closed && !initializeCurrentFile(timestamp)) {
      LOG.info("Waiting for next file creation");
      Thread.sleep(waitTimeForCreate);
      build();
    }
  }

  private void waitForNextFileCreation(String fileName) 
      throws IOException, InterruptedException {
    while (!closed && !setNextHigher(fileName)) {
      LOG.info("Waiting for next file creation");
      Thread.sleep(waitTimeForCreate);
      build();
    }
  }

  public boolean isBeforeStream(String fileName) throws IOException {
    return fileMap.isBefore(fileName);
  }

  static final ThreadLocal<DateFormat> fileFormat = 
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + "-" + "MM" + "-" + "dd" + "-" +
          "HH" + "-" + "mm");
    }    
  };

  static final ThreadLocal<DateFormat> minDirFormat = 
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + File.separator + "MM" +
          File.separator + "dd" + File.separator + "HH" + File.separator +"mm");
    }    
  };

  static final ThreadLocal<DateFormat> hhDirFormat = 
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + File.separator + "MM" +
          File.separator + "dd" + File.separator + "HH");
    }    
  };
}
