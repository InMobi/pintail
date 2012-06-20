package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;

public class CollectorReader extends AbstractPartitionStreamReader {

  private static final Log LOG = LogFactory.getLog(PartitionReader.class);

  private final PartitionId partitionId;
  private final String streamName;
  private final PartitionCheckpoint partitionCheckpoint;
  private Date startTime;
  private LocalStreamCollectorReader lReader;
  private CollectorStreamReader cReader;

  CollectorReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, Cluster cluster,
      String streamName,
      Date startTime, long waitTimeForFlush, boolean noNewFiles)
          throws IOException {
    this.partitionId = partitionId;
    this.startTime = startTime;
    this.streamName = streamName;
    this.partitionCheckpoint = partitionCheckpoint;
    lReader = new LocalStreamCollectorReader(partitionId,  cluster, streamName);
    cReader = new CollectorStreamReader(partitionId, cluster, streamName,
        waitTimeForFlush, noNewFiles);
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

  private void initializeCurrentFileFromCheckpointLocalStream(
      String localStreamFileName) throws IOException, InterruptedException {
    String error = "Checkpoint file does not exist";
    if (!lReader.isEmpty()) {
      if (lReader.initializeCurrentFile(new PartitionCheckpoint(
          localStreamFileName, partitionCheckpoint.getLineNum()))) {
        reader = lReader;
      } else {
        throw new IllegalArgumentException(error);
      } 
    } else if (!cReader.isEmpty()) {
      if (cReader.isBeforeStream(
          CollectorStreamReader.getCollectorFileName(streamName,
              localStreamFileName))) {
        reader = cReader;
        if (!reader.initFromStart()) {
          throw new IllegalArgumentException(error);
        }
      } else {
        throw new IllegalArgumentException(error);
      }
    } else {
      reader = cReader;
      cReader.startFromBegining();
    }
  }

  private void initializeCurrentFileFromCheckpoint() 
      throws IOException, InterruptedException {
    String fileName = partitionCheckpoint.getFileName();
    if (cReader.isCollectorFile(fileName)) {
      if (cReader.initializeCurrentFile(partitionCheckpoint)) {
        reader = cReader;
      } else { //file could be moved to local stream
        String localStreamFileName = 
            LocalStreamCollectorReader.getDatabusStreamFileName(
                partitionId.getCollector(), fileName);
        initializeCurrentFileFromCheckpointLocalStream(localStreamFileName);
      }
    } else if (lReader.isStreamFile(fileName)) {
      LOG.debug("Checkpointed file is in local stream directory");
      initializeCurrentFileFromCheckpointLocalStream(fileName);
    } else {
      LOG.warn("Would never reach here");
    }
  }

  public void initializeCurrentFile() throws IOException, InterruptedException {
      LOG.info("Initializing partition reader's current file");
      cReader.build();

      if (startTime != null) {
        lReader.build(startTime);
        initializeCurrentFileFromTimeStamp(startTime);
      } else if (partitionCheckpoint != null &&
          partitionCheckpoint.getFileName() != null) {
          lReader.build(LocalStreamCollectorReader.getBuildTimestamp(
            streamName, partitionId.getCollector(), partitionCheckpoint));
        initializeCurrentFileFromCheckpoint();
      } else {
        LOG.info("Would never reach here");
      }
      LOG.info("Intialized currentFile:" + reader.getCurrentFile() +
          " currentLineNum:" + reader.getCurrentLineNum());
  }

  public String readLine() throws IOException, InterruptedException {
    assert (reader != null);
    String line = reader.readLine();
    if (line == null) {
      if (reader == lReader) {
        lReader.close();
        LOG.info("Switching to collector stream as we reached end of" +
            " stream on local stream");
        LOG.info("current file:" + reader.getCurrentFile());
        cReader.startFromNextHigher(
            CollectorStreamReader.getCollectorFileName(
                streamName,
                reader.getCurrentFile().getName()));
        reader = cReader;
      } else { // reader should be cReader
        assert (reader == cReader);
        cReader.close();
        LOG.info("Looking for current file in local stream");
        lReader.build(CollectorStreamReader.getDateFromCollectorFile(
            reader.getCurrentFile().getName()));
        if (!lReader.setCurrentFile(
            LocalStreamCollectorReader.getDatabusStreamFileName(
                partitionId.getCollector(),
                cReader.getCurrentFile().getName()),
                cReader.getCurrentLineNum())) {
          LOG.info("Did not find current file in local stream as well") ;
          cReader.startFromNextHigher(
              reader.getCurrentFile().getName());
        } else {
          LOG.info("Switching to local stream as the file got moved");
          reader = lReader;
        }
      }
    }
    return line;
  }

}
