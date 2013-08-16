package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.files.DatabusStreamFile;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.MessageCheckpoint;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class CollectorReader extends AbstractPartitionStreamReader {

  private static final Log LOG = LogFactory.getLog(CollectorReader.class);

  private final PartitionId partitionId;
  private final String streamName;
  private final PartitionCheckpoint partitionCheckpoint;
  private Date startTime;
  private LocalStreamCollectorReader lReader;
  private CollectorStreamReader cReader;
  private final CollectorReaderStatsExposer metrics;
  private boolean isLocalStreamAvailable = false;
  private boolean shouldBeClosed = false;

  CollectorReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, FileSystem fs,
      String streamName,
      Path collectorDir, Path streamsLocalDir,
      Configuration conf,
      Date startTime, long waitTimeForFlush,
      long waitTimeForFileCreate, CollectorReaderStatsExposer metrics,
      boolean noNewFiles, Date stopTime)
          throws IOException {
    this.partitionId = partitionId;
    this.startTime = startTime;
    this.streamName = streamName;
    this.partitionCheckpoint = partitionCheckpoint;
    this.metrics = metrics;
    if (streamsLocalDir != null) {
      lReader = new LocalStreamCollectorReader(partitionId,  fs, streamName,
          streamsLocalDir, conf, waitTimeForFileCreate, metrics, stopTime);
      isLocalStreamAvailable = true;
    }
    cReader = new CollectorStreamReader(partitionId, fs, streamName,
        collectorDir, waitTimeForFlush, waitTimeForFileCreate, metrics,
        conf, noNewFiles, stopTime, isLocalStreamAvailable);
  }

  private void initializeCurrentFileFromTimeStamp(Date timestamp)
      throws IOException, InterruptedException {
    if (lReader.initializeCurrentFile(timestamp)) {
      reader = lReader;
    } else {
      reader = cReader;
      LOG.debug("Did not find the file associated with timestamp");
      cReader.startFromTimestmp(timestamp);
    }
  }

  /*
   * close the reader if given stopTime is beyond the checkpoint
   */
  private void initializeCurrentFileFromCheckpointLocalStream(
      String localStreamFileName) throws IOException, InterruptedException {
    String error = "Checkpoint file does not exist";
    if (!lReader.isEmpty()) {
      if (lReader.initializeCurrentFile(new PartitionCheckpoint(
          DatabusStreamFile.create(streamName, localStreamFileName),
          partitionCheckpoint.getLineNum()))) {
        reader = lReader;
      } else if (lReader.isStopped() || cReader.isStopped()) {
        shouldBeClosed  = true;
      } else {
        throw new IllegalArgumentException(error);
      }
    } else {
      reader = cReader;
      String collectorFileName = CollectorStreamReader.getCollectorFileName(
          streamName, localStreamFileName);
      initializeCurrentFileFromFailedCheckpoint(collectorFileName);
    }
  }

  private void initializeCurrentFileFromFailedCheckpoint(
      String collectorFileName)
      throws IOException, InterruptedException {
    String error = "Checkpoint file does not exist";
    if (!cReader.isEmpty()) {
      if (cReader.isBeforeStream(collectorFileName)) {
        reader.initFromStart();
      } else if (checkAnyReaderIsStopped()) {
        shouldBeClosed  = true;
      } else {
        throw new IllegalArgumentException(error);
      }
    } else {
      if (checkAnyReaderIsStopped()) {
        shouldBeClosed = true;
      } else {
        cReader.startFromBegining();
      }
    }
  }

  private boolean checkAnyReaderIsStopped() {
    return cReader.isStopped()
        || (isLocalStreamAvailable && lReader.isStopped());
  }

  private void initializeCurrentFileFromCheckpoint()
      throws IOException, InterruptedException {
    String fileName = partitionCheckpoint.getFileName();
    if (CollectorStreamReader.isCollectorFile(fileName)) {
      if (cReader.initializeCurrentFile(partitionCheckpoint)) {
        reader = cReader;
      } else { //file could be moved to local stream
        reader = lReader;
        String localStreamFileName =
            LocalStreamCollectorReader.getDatabusStreamFileName(
                partitionId.getCollector(), fileName);
        initializeCurrentFileFromCheckpointLocalStream(localStreamFileName);
      }
    } else {
      reader = lReader;
      LOG.debug("Checkpointed file is in local stream directory");
      initializeCurrentFileFromCheckpointLocalStream(fileName);
    }
  }

  private void initializeCurrentFileFromStartOfStream()
      throws IOException, InterruptedException {
    if (!lReader.isEmpty()) {
      reader = lReader;
    } else {
      reader = cReader;
    }

    reader.startFromBegining();
  }

  public void initializeCurrentFile() throws IOException, InterruptedException {
    LOG.info("Initializing partition reader's current file");
    cReader.build();

    if (isLocalStreamAvailable) {
      if (partitionCheckpoint != null) {
        lReader.build(LocalStreamCollectorReader.getBuildTimestamp(streamName,
            partitionId.getCollector(), partitionCheckpoint));
        initializeCurrentFileFromCheckpoint();
      } else if (startTime != null) {
        lReader.build(startTime);
        initializeCurrentFileFromTimeStamp(startTime);
      } else {
        lReader.build(null);
        initializeCurrentFileFromStartOfStream();
      }
    } else {
      reader = cReader;
      initializeCurrentFileFromCollectorStreamOnly();
    }
    LOG.info("Intialized currentFile:" + reader.getCurrentFile()
        + " currentLineNum:" + reader.getCurrentLineNum());
  }

  private void initializeCurrentFileFromCollectorStreamOnly()
      throws IOException, InterruptedException {
    if (partitionCheckpoint != null) {
      if (!reader.initializeCurrentFile(partitionCheckpoint)) {
        initializeCurrentFileFromFailedCheckpoint(partitionCheckpoint
            .getFileName());
      }
    } else if (startTime != null) {
      reader.startFromTimestmp(startTime);
    } else {
      reader.startFromBegining();
    }
  }

  public Message readLine() throws IOException, InterruptedException {
    assert (reader != null);
    Message line = super.readLine();
    while (line == null) {
      if (closed) {
        return line;
      }

      // check whether readers are stopped
      if (reader.isStopped()) {
        return null;
      }
      if (reader == lReader) {
        lReader.closeStream();
        LOG.info("Switching to collector stream as we reached end of"
            + " stream on local stream");
        LOG.info("current file:" + reader.getCurrentFile());
        cReader.startFromNextHigher(
            CollectorStreamReader.getCollectorFileName(
                streamName,
                reader.getCurrentFile().getName()));
        reader = cReader;
        metrics.incrementSwitchesFromLocalToCollector();
      } else { // reader should be cReader
        assert (reader == cReader);
        cReader.closeStream();
        LOG.info("Looking for current file in local stream");
        lReader.build(CollectorStreamReader.getDateFromCollectorFile(
            reader.getCurrentFile().getName()));
        if (!lReader.setCurrentFile(
            LocalStreamCollectorReader.getDatabusStreamFileName(
                partitionId.getCollector(),
                cReader.getCurrentFile().getName()),
                cReader.getCurrentLineNum())) {
          LOG.info("Did not find current file in local stream as well");
          cReader.startFromNextHigher(
              reader.getCurrentFile().getName());
        } else {
          LOG.info("Switching to local stream as the file got moved");
          reader = lReader;
          metrics.incrementSwitchesFromCollectorToLocal();
        }
      }
      boolean ret = reader.openStream();
      if (ret) {
        LOG.info("Reading file " + reader.getCurrentFile()
            + " and lineNum:" + reader.getCurrentLineNum());
        line = super.readLine();
      } else {
        return null;
      }
    }
    return line;
  }

  @Override
  public MessageCheckpoint getMessageCheckpoint() {
    return new PartitionCheckpoint(reader.getCurrentStreamFile(),
        reader.getCurrentLineNum());
  }

  @Override
  public boolean shouldBeClosed() {
    return shouldBeClosed;
  }

  @Override
  public MessageCheckpoint buildStartPartitionCheckpoints() {
    return null;
  }
}
