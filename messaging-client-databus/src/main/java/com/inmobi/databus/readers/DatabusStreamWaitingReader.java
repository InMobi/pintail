package com.inmobi.databus.readers;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.files.DatabusStreamFile;
import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.partition.PartitionId;

public abstract class DatabusStreamWaitingReader extends DatabusStreamReader {

  DatabusStreamWaitingReader(PartitionId partitionId, Cluster cluster,
      String streamName) throws IOException {
    super(partitionId, cluster, streamName);
  }

  private static final Log LOG = LogFactory.getLog(DatabusStreamWaitingReader.class);
  
  @Override
  public String readLine() throws IOException, InterruptedException {
    String line = null;
    if (inStream != null) {
      line = readLine(inStream, reader);
    }
    while (line == null) { // reached end of file
      if (!nextFile()) { // reached end of file list
        LOG.info("could not find next file. Rebuilding");
        build(getDateFromDatabusStreamFile(streamName, 
            currentFile.getName()));
        if (!nextFile()) { // reached end of stream
          if (noNewFiles) {
            // this boolean check is only for tests 
            return null;
          } 
          LOG.info("Could not find next file");
          startFromNextHigher(currentFile.getName());
        } else {
          LOG.info("Reading from " + currentFile + " after rebuild");
        }
      } else {
        // read line from next file
        LOG.info("Reading from next file " + currentFile);
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
