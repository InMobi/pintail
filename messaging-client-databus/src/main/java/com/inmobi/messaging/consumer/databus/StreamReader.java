package com.inmobi.messaging.consumer.databus;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.Cluster;

abstract class StreamReader {

  private static final Log LOG = LogFactory.getLog(StreamReader.class);

  protected String streamName;
  protected TreeMap<String, Path> files = new TreeMap<String, Path>();
  protected Iterator<String> fileNameIterator;
  protected PathFilter pathFilter;
  protected Date timestamp;
  protected PartitionCheckpoint checkpoint;
  protected Cluster cluster;
  protected PartitionId partitionId;
  protected Path currentFile;
  protected long currentLineNum = 0;
  protected long numLinesTobeSkipped;
  protected FileSystem fs;

  protected void init(PartitionId partitionId, Cluster cluster, 
      String streamName) throws IOException {
    this.streamName = streamName;
    this.cluster = cluster;
    this.fs = FileSystem.get(cluster.getHadoopConf());
  }
  
  protected abstract void build() throws IOException;
  
  protected FSDataInputStream inStream;
  protected BufferedReader reader;

  void openStream() throws IOException {
    openCurrentFile(false);
  }

  void close() throws IOException {
    closeCurrentFile();
    files.clear();
  }

  protected void openCurrentFile(boolean next) throws IOException {
    closeCurrentFile();
    if (next) {
      numLinesTobeSkipped = 0;
    } else {
      numLinesTobeSkipped = currentLineNum;  
    }
    resetCurrentFileSettings();
    LOG.debug("Opening file:" + currentFile + " lineNum" + currentLineNum);
    inStream = fs.open(currentFile);
    reader = getReader(inStream);
  }
  
  private void closeReader() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  private void closeCurrentFile() throws IOException {
    closeReader();
    if (inStream != null) {
      inStream.close();
    }
  }

  protected boolean setIterator() {
    fileNameIterator = files.navigableKeySet().iterator();
    if (currentFile != null) {
      while (fileNameIterator.hasNext()) {
        String file = fileNameIterator.next();
        if (currentFile.getName().equals(file)) {
          return true;
        }
      }
    }
    return false;
  }

  boolean initializeCurrentFile(Date timestamp) throws Exception {
    resetCurrentFile();
    this.timestamp = timestamp;
    currentFile = getFileForTimeStamp(timestamp);
    if (currentFile != null) {
      setIterator();
      LOG.debug("CurrentFile:" + currentFile + " currentLineNum:"+ currentLineNum);
    }
    return currentFile != null;
  }

  boolean initializeCurrentFile(PartitionCheckpoint checkpoint) throws Exception {
    resetCurrentFile();
    this.checkpoint = checkpoint;
    LOG.debug("checkpoint:" + checkpoint);
    currentFile = getFileForCheckpoint(checkpoint);
    if (currentFile != null) {
      currentLineNum = checkpoint.getLineNum();
      LOG.debug("CurrentFile:" + currentFile + " currentLineNum:"+ currentLineNum);
      setIterator();
    } 
    return currentFile != null;
  }

  boolean initFromStart() throws Exception {
    resetCurrentFile();
    currentFile = getFirstFile();
    
    if (currentFile != null) {
      LOG.debug("CurrentFile:" + currentFile + " currentLineNum:"+ currentLineNum);
      setIterator();
    }
    return currentFile != null;
  }

  void resetCurrentFile() {
    currentFile = null;
    resetCurrentFileSettings();
  }
  boolean setNextHigher() throws IOException {
    if (currentFile != null) {
      Map.Entry<String, Path> higherEntry = files.higherEntry(currentFile.getName());
      if (higherEntry != null) {
        currentFile = higherEntry.getValue();
        setIterator();
        openCurrentFile(true);
      }
    }
    return false;
  }

  Path getCurrentFile() {
    return currentFile;
  }

  long getCurrentLineNum() {
    return currentLineNum;
  }


  protected abstract Path getFileForCheckpoint(PartitionCheckpoint checkpoint)
      throws Exception;

  protected abstract Path getFileForTimeStamp(Date timestamp)
      throws Exception;

  protected abstract BufferedReader createReader(FSDataInputStream in)
      throws IOException;

  /** 
   * Returns null when reached end of stream 
   */
  abstract String readLine() throws Exception;

  /**
   * Skip the number of lines passed.
   * 
   * @return the actual number of lines skipped.
   */
  private long skipLines(FSDataInputStream in, BufferedReader reader, 
      long numLines) 
      throws IOException {
    long lineNum = 0;
    while (lineNum != numLines) {
      String line = readLine(in, reader);
      if (line == null) {
        break;
      }
      lineNum++;
    }
    LOG.debug("Skipped " + lineNum + " lines");
    if (lineNum != numLines) {
      LOG.warn("Skipped wrong number of lines");
    }
    return lineNum;
  }
  
  String readLine(FSDataInputStream in, BufferedReader reader)
      throws IOException {
    String line = reader.readLine();
    if (line != null) {
      currentLineNum++;
    }
    return line;
  }

  BufferedReader getReader(FSDataInputStream in) throws IOException {
    BufferedReader reader = createReader(in);
    skipLines(in, reader, numLinesTobeSkipped);
    return reader;
  }

  private void resetCurrentFileSettings() {
    currentLineNum = 0;
  }
  
  boolean nextFile() throws IOException {
    if (fileNameIterator.hasNext()) {
      String fileName = fileNameIterator.next();
      LOG.debug("next file name:" + fileName);
      currentFile = files.get(fileName);
      openCurrentFile(true);
      return true;
    }
    return false;
  }

  Path getFirstFile()
      throws IOException {
    Map.Entry<String, Path> first = files.firstEntry();
    if (first != null) {
      return first.getValue();
    }
    return null;
  }

  Path getLastFile() throws IOException {
    Map.Entry<String, Path> last = files.lastEntry();
    if (last != null) {
      return last.getValue();
    }
    return null;
  }

  static int getIndexOf(String str, int ch, int occurance) {
    int first = str.indexOf(ch);
    if (occurance == 1) {
      return first;
    } else {
      return (first + 1) + getIndexOf(str.substring(first + 1), ch, occurance - 1);
    }
  }

  static DateFormat dateFormat = new SimpleDateFormat("yyyy" + "-" + "MM" + "-" +
  "dd" + "-" + "HH" + "-" + "mm");

  static Date getDate(String fileName, int occur) throws Exception {
    String dateStr = fileName.substring(getIndexOf(fileName, '-', occur) + 1,
                         fileName.indexOf("_"));
    return dateFormat.parse(dateStr);  
  }

}
