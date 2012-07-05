package com.inmobi.databus.readers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.files.CollectorFile;
import com.inmobi.databus.files.DatabusStreamFile;
import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.partition.PartitionId;

public class CollectorStreamReader extends StreamReader<CollectorFile> {

  private static final Log LOG = LogFactory.getLog(CollectorStreamReader.class);

  private long waitTimeForFlush;
  protected long currentOffset = 0;
  private boolean sameStream = false;

  public CollectorStreamReader(PartitionId partitionId,
      Cluster cluster, String streamName, long waitTimeForFlush,
      long waitTimeForCreate,
      boolean noNewFiles) throws IOException {
    super(partitionId, cluster, streamName);
    this.waitTimeForFlush = waitTimeForFlush;
    this.waitTimeForCreate = waitTimeForCreate;
    this.noNewFiles = noNewFiles;
    LOG.info("Collector reader initialized with partitionId:" + partitionId +
        " streamDir:" + streamDir + 
        " waitTimeForFlush:" + waitTimeForFlush +
        " waitTimeForCreate:" + waitTimeForCreate);
  }

  protected void initCurrentFile() {
    super.initCurrentFile();
    sameStream = false;
  }

  protected Path getStreamDir(Cluster cluster, String streamName) {
    Path streamDataDir = new Path(cluster.getDataDir(), streamName);
    return new Path(streamDataDir, partitionId.getCollector());
  }

  protected FileMap<CollectorFile> createFileMap() throws IOException {
    return new FileMap<CollectorFile>() {
      
      @Override
      protected PathFilter createPathFilter() {
        return new PathFilter() {
          @Override
          public boolean accept(Path p) {
            if (p.getName().endsWith("_current")
                || p.getName().endsWith("_stats")) {
              return false;
            }
            return true;
          }
        };
      }

      @Override
      protected void buildList() throws IOException {
        if (fs.exists(streamDir)) {
          FileStatus[] fileStatuses = fs.listStatus(streamDir, pathFilter);
          if (fileStatuses == null || fileStatuses.length == 0) {
            LOG.info("No files in directory:" + streamDir);
            return;
          }
          for (FileStatus file : fileStatuses) {
            addPath(file);
          }
        } else {
          LOG.info("Collector directory does not exist");
        }
      }

      @Override
      protected TreeMap<CollectorFile, FileStatus> createFilesMap() {
        return new TreeMap<CollectorFile, FileStatus>();
      }

      @Override
      protected CollectorFile getStreamFile(String fileName) {
        return CollectorFile.create(fileName);
      }

      @Override
      protected CollectorFile getStreamFile(FileStatus file) {
        return CollectorFile.create(file.getPath().getName());
      }

    };
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
      LOG.info("Seeking to offset:" + currentOffset);
      seekToOffset(in, reader);
    } else {
      skipLines(in, reader, currentLineNum);
      sameStream = true;
    }
  }

  private void seekToOffset(FSDataInputStream in, BufferedReader reader) 
      throws IOException {
    in.seek(currentOffset);
  }

  protected String readLine(FSDataInputStream in, BufferedReader reader)
      throws IOException {
    String line = null;
    if (inStream != null) {
      line = super.readLine(inStream, reader);
      currentOffset = inStream.getPos();
    }
    return line;
  }
  
  public String readLine() throws IOException, InterruptedException {
    String line = readLine(inStream, reader);
    while (line == null) { // reached end of file?
      if (closed) {
        LOG.info("Stream closed");
        break;
      }
      LOG.info("Read " + currentFile + " with lines:" + currentLineNum);
      build(); // rebuild file list
      if (!nextFile()) { //there is no next file
        if (noNewFiles) {
          // this boolean check is only for tests 
          return null;
        } 
        if (!setIterator()) {
          LOG.info("Could not find current file in the stream");
          if (isWithinStream(getCurrentFile().getName())) {
            LOG.info("Staying in collector stream as earlier files still exist");
            startFromNextHigherAndOpen(getCurrentFile().getName());
            LOG.info("Reading from the next higher file");
          } else {
            LOG.info("Current file would have been moved to Local Stream");
            return null;
          }
        } else {
          waitForFlushAndReOpen();
          LOG.info("Reading from the same file after reopen");
        }
      } else {
        LOG.info("Reading from next file: " + currentFile);
      }
      line = readLine(inStream, reader);
    }
    return line;
  }

  private void waitForFlushAndReOpen() 
      throws IOException, InterruptedException {
    if (!closed) {
      LOG.info("Waiting for flush");
      Thread.sleep(waitTimeForFlush);
      openCurrentFile(false);
    }
  }

  private void startFromNextHigherAndOpen(String fileName) 
      throws IOException, InterruptedException {
    boolean ret = startFromNextHigher(fileName);
    if (ret) {
      openCurrentFile(true);
    }
  }

  public boolean startFromNextHigher(String fileName) 
      throws IOException, InterruptedException {
    if (!setNextHigher(fileName)) {
      if (noNewFiles) {
        // this boolean check is only for tests 
        return false;
      }
      waitForNextFileCreation(fileName);
    }
    return true;
  }

  private void waitForNextFileCreation(String fileName) 
      throws IOException, InterruptedException {
    while (!closed && !setNextHigher(fileName)) {
      LOG.info("Waiting for next file creation");
      Thread.sleep(waitTimeForCreate);
      build();
    }
  }

  @Override
  protected String getStreamFileName(String streamName, Date timestamp) {
    return getCollectorFileName(streamName, timestamp);
  }

  public boolean isStreamFile(String fileName) {
    return isCollectorFile(fileName);
  }

  public boolean isCollectorFile(String fileName) {
    try {
      CollectorFile.create(fileName);
    } catch (IllegalArgumentException ie) {
      return false;
    }
    return true;
  }

  public static String getCollectorFileName(String streamName,
      String localStreamfile) {
    return DatabusStreamFile.create(streamName, localStreamfile)
        .getCollectorFile().toString();
  }

  public static Date getDateFromCollectorFile(String fileName)
      throws IOException {
    return CollectorFile.create(fileName).getTimestamp();
  }

  public static String getCollectorFileName(String streamName, Date date) {
    return new CollectorFile(streamName, date, 0).toString();
  }

}
