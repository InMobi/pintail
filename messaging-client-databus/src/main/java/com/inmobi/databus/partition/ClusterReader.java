package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;
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
    Map<Integer, PartitionCheckpoint> listOfCheckpoints =
        partitionCheckpointList.getCheckpoints();

    if (listOfCheckpoints != null) {
      Collection<PartitionCheckpoint> listofPartitionCheckpoints =
          listOfCheckpoints.values();
      Iterator<PartitionCheckpoint> it = listofPartitionCheckpoints.iterator();
      Date timeStamp = null;
      if (it.hasNext()) {
        partitioncheckpoint = it.next();
        if (partitioncheckpoint != null) {
          timeStamp = DatabusStreamWaitingReader.getDateFromCheckpointPath(
              partitioncheckpoint.getFileName());
        }
      }
      while (it.hasNext()) {
        PartitionCheckpoint tmpPartitionCheckpoint = it.next();
        if (tmpPartitionCheckpoint != null) {
          Date date = DatabusStreamWaitingReader.getDateFromCheckpointPath(
              tmpPartitionCheckpoint.getFileName());
          if (timeStamp.compareTo(date) > 0) {
            partitioncheckpoint = tmpPartitionCheckpoint;
            timeStamp = date;
          }
        }
      }
    }
    return partitioncheckpoint;
  }

  public void initializeCurrentFile() throws IOException, InterruptedException {
    LOG.info("Initializing partition reader's current file");

    if (leastPartitionCheckpoint != null) {
      LOG.info("Least partition checkpoint " + leastPartitionCheckpoint);
      ((DatabusStreamWaitingReader) reader).build(buildTimestamp);
      if (!reader.isEmpty()) {
        /*
        If the partition checkpoint is completed checkpoint (i.e. line
        number is -1) or if it the filename of the checkpoint is null (
        when the checkpointing was done partially or before a single
        message was read) then it has to start from the next checkpoint.
        */
        if (leastPartitionCheckpoint.getLineNum() == -1 || leastPartitionCheckpoint
            .getName() == null) {
          ((DatabusStreamWaitingReader) reader).initFromNextCheckPoint();
        } else if (!reader.initializeCurrentFile(leastPartitionCheckpoint)) {
          throw new IllegalArgumentException("Checkpoint file does not exist");
        }
      } else {
        reader.startFromBegining();
      }
    } else if (startTime != null) {
      ((DatabusStreamWaitingReader) reader).build(startTime);
      if (!reader.initializeCurrentFile(startTime)) {
        LOG.debug("Did not find the file associated with timestamp");
        reader.startFromTimestmp(startTime);
      }
    } else {
      // starting from start of the stream. Here, buildTimestamp is null if the
      // stream is empty
      ((DatabusStreamWaitingReader) reader).build(buildTimestamp);
      reader.startFromBegining();
    }
    LOG.info("Intialized currentFile:" + reader.getCurrentFile()
        + " currentLineNum:" + reader.getCurrentLineNum());
  }

  @Override
  public MessageCheckpoint getMessageCheckpoint() {
    DatabusStreamWaitingReader dataWaitingReader =
        (DatabusStreamWaitingReader) reader;
    DeltaPartitionCheckPoint consumerPartitionCheckPoint =
        new DeltaPartitionCheckPoint(dataWaitingReader.getCurrentStreamFile(),
            dataWaitingReader.getCurrentLineNum(), dataWaitingReader.
            getCurrentMin(), dataWaitingReader.getDeltaCheckpoint());
    dataWaitingReader.resetDeltaCheckpoint();
    return consumerPartitionCheckPoint;
  }

  @Override
  public boolean shouldBeClosed() {
    return false;
  }

  public MessageCheckpoint buildStartPartitionCheckpoints() {
    DatabusStreamWaitingReader dataWaitingReader =
        (DatabusStreamWaitingReader) reader;
    return new DeltaPartitionCheckPoint(
        dataWaitingReader.buildStartPartitionCheckpoints());
  }
}
