package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.MessageCheckpoint;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class ClusterReader extends AbstractPartitionStreamReader {

  private static final Log LOG = LogFactory.getLog(PartitionReader.class);

  private final Date startTime;
  private final Path streamDir;
  private PartitionCheckpoint leastPartitionCheckpoint;
  private Date buildTimestamp;

  ClusterReader(PartitionId partitionId,
      PartitionCheckpointList partitionCheckpointList, FileSystem fs,
      Path streamDir, Configuration conf, String inputFormatClass,
      Date startTime, long waitTimeForFileCreate, boolean isDatabusData,
      PartitionReaderStatsExposer metrics, boolean noNewFiles,
      Set<Integer> partitionMinList, Date stopTime)
          throws IOException {
    this.startTime = startTime;
    this.streamDir = streamDir;

    reader = new DatabusStreamWaitingReader(partitionId, fs, streamDir,
        inputFormatClass, conf, waitTimeForFileCreate, metrics, noNewFiles,
        partitionMinList, partitionCheckpointList, stopTime);

    initializeBuildTimeStamp(partitionCheckpointList);
  }

  private void initializeBuildTimeStamp(
      PartitionCheckpointList partitionCheckpointList) throws IOException {
    if (partitionCheckpointList != null) {
      leastPartitionCheckpoint = findLeastPartitionCheckPointTime(
          partitionCheckpointList);
    }
    if (leastPartitionCheckpoint != null) {
      buildTimestamp = DatabusStreamWaitingReader.
          getBuildTimestamp(streamDir, leastPartitionCheckpoint);
    } else if (startTime != null) {
      buildTimestamp = startTime;
    } else {
      buildTimestamp = ((DatabusStreamWaitingReader) reader).
          getTimestampFromStartOfStream(null);
    }
    ((DatabusStreamWaitingReader) reader).initializeBuildTimeStamp(buildTimestamp);
  }

  /*
   * this method is used to find the partition checkpoint which has least
   * time stamp.
   * So that reader starts build listing from this partition checkpoint
   * time stamp).
   */
  public static PartitionCheckpoint findLeastPartitionCheckPointTime(
      PartitionCheckpointList partitionCheckpointList) {
    PartitionCheckpoint partitioncheckpoint = null;
    Iterator<PartitionCheckpoint> it = partitionCheckpointList.getCheckpoints().
        values().iterator();
    Date leastPckTimeStamp = null;
    if (it.hasNext()) {
      partitioncheckpoint = it.next();
      if (partitioncheckpoint != null) {
        leastPckTimeStamp = DatabusStreamWaitingReader.getDateFromCheckpointPath(
            partitioncheckpoint.getFileName());
      }
    }
    while (it.hasNext()) {
      PartitionCheckpoint tmpPartitionCheckpoint = it.next();
      if (tmpPartitionCheckpoint != null) {
        Date pckTimeStamp = DatabusStreamWaitingReader
            .getDateFromCheckpointPath(tmpPartitionCheckpoint.getFileName());
        if (pckTimeStamp.before(leastPckTimeStamp)) {
          partitioncheckpoint = tmpPartitionCheckpoint;
          leastPckTimeStamp = pckTimeStamp;
        }
      }
    }
    return partitioncheckpoint;
  }

  public void initializeCurrentFile() throws IOException, InterruptedException {
    LOG.info("Initializing partition reader's current file");

    // Build the reader from buildTimestamp
    ((DatabusStreamWaitingReader) reader).build(buildTimestamp);

    if (leastPartitionCheckpoint != null) {
      LOG.info("Least partition checkpoint " + leastPartitionCheckpoint);
      if (!reader.isEmpty()) {
        ((DatabusStreamWaitingReader) reader).startFromCheckPoint();
      } else {
        reader.startFromBegining();
      }
    } else if (startTime != null) {
      if (!reader.initializeCurrentFile(startTime)) {
        LOG.debug("Did not find the file associated with timestamp");
        reader.startFromTimestmp(startTime);
      }
    } else {
      // starting from start of the stream. Here, buildTimestamp is null if the
      // stream is empty
      reader.startFromBegining();
    }
    LOG.info("Intialized currentFile:" + reader.getCurrentFile()
        + " currentLineNum:" + reader.getCurrentLineNum());
  }

  @Override
  public MessageCheckpoint getMessageCheckpoint() {
    DatabusStreamWaitingReader dataWaitingReader =
        (DatabusStreamWaitingReader) reader;
    /*
     * current file will be null only if there are no files in the stream
     *  for a given stop time.
     */
    if (dataWaitingReader.getCurrentFile() == null) {
      return null;
    }
    DeltaPartitionCheckPoint consumerPartitionCheckPoint =
        new DeltaPartitionCheckPoint(dataWaitingReader.getCurrentStreamFile(),
            dataWaitingReader.getCurrentLineNum(), dataWaitingReader.
            getCurrentMin(), dataWaitingReader.getDeltaCheckpoint());
    dataWaitingReader.resetDeltaCheckpoint();
    return consumerPartitionCheckPoint;
  }

  public MessageCheckpoint buildStartPartitionCheckpoints() {
    DatabusStreamWaitingReader dataWaitingReader =
        (DatabusStreamWaitingReader) reader;
    return new DeltaPartitionCheckPoint(
        dataWaitingReader.buildStartPartitionCheckpoints());
  }
}
