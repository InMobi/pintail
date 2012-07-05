  package com.inmobi.databus.readers;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.files.DatabusStreamFile;
import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.partition.PartitionId;

public class DatabusStreamWaitingReader extends DatabusStreamReader {

  private static final Log LOG = LogFactory.getLog(
      DatabusStreamWaitingReader.class);

  public DatabusStreamWaitingReader(PartitionId partitionId, FileSystem fs,
      String streamName,  Path streamDir, long waitTimeForCreate,
      boolean noNewFiles) throws IOException {
    super(partitionId, fs, streamName, streamDir, noNewFiles);
    this.waitTimeForCreate = waitTimeForCreate;
  }
  
  protected void startFromNextHigher(FileStatus file)
      throws IOException, InterruptedException {
    if (!setNextHigherAndOpen(file)) {
      if (noNewFiles) {
        // this boolean check is only for tests 
        return;
      }
      waitForNextFileCreation(file);
    }
  }

  private void waitForNextFileCreation(FileStatus file)
      throws IOException, InterruptedException {
    while (!closed && !setNextHigherAndOpen(file)) {
      LOG.info("Waiting for next file creation");
      Thread.sleep(waitTimeForCreate);
      build();
    }
  }

  @Override
  public String readLine() throws IOException, InterruptedException {
    String line = null;
    if (inStream != null) {
      line = readLine(inStream, reader);
    }
    while (line == null) { // reached end of file
      if (closed) {
        LOG.info("Stream closed");
        break;
      }
      LOG.info("Read " + getCurrentFile() + " with lines:" + currentLineNum);
      if (!nextFile()) { // reached end of file list
        LOG.info("could not find next file. Rebuilding");
        build(getDateFromDatabusStreamDir(streamDir, 
            getCurrentFile()));
        if (!nextFile()) { // reached end of stream
          if (noNewFiles) {
            // this boolean check is only for tests 
            return null;
          } 
          LOG.info("Could not find next file");
          startFromNextHigher(currentFile);
          LOG.info("Reading from next higher file "+ getCurrentFile());
        } else {
          LOG.info("Reading from " + getCurrentFile() + " after rebuild");
        }
      } else {
        // read line from next file
        LOG.info("Reading from next file " + getCurrentFile());
      }
      line = readLine(inStream, reader);
    }
    return line;
  }

  @Override
  protected FileMap<DatabusStreamFile> createFileMap() throws IOException {
    return new StreamFileMap() {

      @Override
      protected PathFilter createPathFilter() {
        return new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return true;
          }          
        };
      }
      
    };
  }

}
