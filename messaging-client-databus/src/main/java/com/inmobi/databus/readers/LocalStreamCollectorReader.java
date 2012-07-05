package com.inmobi.databus.readers;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.files.DatabusStreamFile;
import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;

public class LocalStreamCollectorReader extends DatabusStreamReader {

  private static final Log LOG = LogFactory.getLog(
      LocalStreamCollectorReader.class);

  private final String collector;
  
  public LocalStreamCollectorReader(PartitionId partitionId, 
      Cluster cluster, String streamName) throws IOException {
    super(partitionId, cluster, streamName);
    this.collector = partitionId.getCollector();
  }
  
  public FileMap<DatabusStreamFile> createFileMap() throws IOException {
    return new StreamFileMap() {
      @Override
      protected PathFilter createPathFilter() {
        return new PathFilter() {
          @Override
          public boolean accept(Path p) {
            if (p.getName().startsWith(collector)) {
              return true;
            }
            return false;
          }          
        };
      }
    };
  }
  
  public String readLine() throws IOException {
    String line = null;
    if (inStream != null) {
      line = readLine(inStream, reader);
    }
    while (line == null) { // reached end of file
      if (closed) {
        LOG.info("Stream closed");
        break;
      }
      LOG.info("Read " + currentFile + " with lines:" + currentLineNum);
      if (!nextFile()) { // reached end of file list
        LOG.info("could not find next file. Rebuilding");
        build(getDateFromDatabusStreamFile(streamName,
        getCurrentFile().getName()));
        if (!setIterator()) {
          LOG.info("Could not find current file in the stream");
          // set current file to next higher entry
          if (!setNextHigherAndOpen(currentFile)) {
            LOG.info("Could not find next higher entry for current file");
            return null;
          } else {
            // read line from next higher file
            LOG.info("Reading from " + currentFile + ". The next higher file" +
                " after rebuild");
          }
        } else if (!nextFile()) { // reached end of stream
          LOG.info("Reached end of stream");
          return null;
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

  public static Date getBuildTimestamp(String streamName, String collectorName,
      PartitionCheckpoint partitionCheckpoint) {
    String fileName = null;
    if (partitionCheckpoint != null) {
      fileName = partitionCheckpoint.getFileName();
      if (fileName != null && 
          !isDatabusStreamFile(streamName, fileName)) {
        fileName = getDatabusStreamFileName(collectorName, fileName);
      }
    }
    return getBuildTimestamp(streamName, fileName);
  }

  @Override
  protected Path getStreamDir(Cluster cluster, String streamName) {
    return new Path(cluster.getLocalFinalDestDirRoot(), streamName);
  }

}
