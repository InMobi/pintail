package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.databus.readers.LocalStreamReader;
import com.inmobi.databus.readers.MergedStreamReader;

public class ClusterReader extends AbstractPartitionStreamReader {

  private static final Log LOG = LogFactory.getLog(PartitionReader.class);

  private final String streamName;
  private final PartitionCheckpoint partitionCheckpoint;
  private Date startTime;

  ClusterReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, Cluster cluster,
      String streamName,
      Date startTime, boolean isLocal, boolean noNewFiles)
          throws IOException {
    this.startTime = startTime;
    this.streamName = streamName;
    this.partitionCheckpoint = partitionCheckpoint;

    // initialize cluster and its directories
    if (isLocal) {
      reader = new LocalStreamReader(partitionId,  cluster, streamName,
          noNewFiles);
    } else {
      reader = new MergedStreamReader(partitionId, cluster, streamName,
          noNewFiles);
    }
  }

  public void initializeCurrentFile() throws IOException, InterruptedException {
    LOG.info("Initializing partition reader's current file");
    if (startTime != null) {
      ((DatabusStreamWaitingReader)reader).build(startTime);
      if (!reader.initializeCurrentFile(startTime)) {
        LOG.debug("Did not find the file associated with timestamp");
        reader.startFromTimestmp(startTime);
      }
    } else if (partitionCheckpoint != null &&
        partitionCheckpoint.getFileName() != null) {
      ((DatabusStreamWaitingReader)reader).build(
          DatabusStreamWaitingReader.getBuildTimestamp(streamName,
          partitionCheckpoint.getFileName()));
      if (!reader.isEmpty()) {
        if (!reader.initializeCurrentFile(partitionCheckpoint)) {
          throw new IllegalArgumentException("Checkpoint file does not exist");
        }
      } else {
        reader.startFromBegining();
      }
    } else {
      LOG.info("Would never reach here");
    }
    LOG.info("Intialized currentFile:" + reader.getCurrentFile() +
        " currentLineNum:" + reader.getCurrentLineNum());
  }
}
