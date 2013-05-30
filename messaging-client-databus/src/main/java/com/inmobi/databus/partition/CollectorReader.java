package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.files.DatabusStreamFile;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.MessageCheckpoint;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class CollectorReader extends AbstractPartitionStreamReader {

  private static final Log LOG = LogFactory.getLog(PartitionReader.class);

  private final PartitionId partitionId;
  private final String streamName;
  private final PartitionCheckpoint partitionCheckpoint;
  private Date startTime;
  private LocalStreamCollectorReader lReader;
  private CollectorStreamReader cReader;
  private final CollectorReaderStatsExposer metrics;
  private Date stopTime;
  private boolean noNewFiles;

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
    this.stopTime = stopTime;
    this.metrics = metrics;
    this.noNewFiles = noNewFiles;
    lReader = new LocalStreamCollectorReader(partitionId,  fs, streamName,
        streamsLocalDir, conf, waitTimeForFileCreate, metrics, stopTime);
    cReader = new CollectorStreamReader(partitionId, fs, streamName,
        collectorDir, waitTimeForFlush, waitTimeForFileCreate, metrics,
        conf, noNewFiles, stopTime);
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
    } else if (!cReader.isEmpty()) {
      if (cReader.isBeforeStream(
          CollectorStreamReader.getCollectorFileName(streamName,
              localStreamFileName))) {
        reader = cReader;
        if (!reader.initFromStart()) {
          throw new IllegalArgumentException(error);
        }
      } else if (cReader.isStopped() || lReader.isStopped()) {
        shouldBeClosed  = true;
      } else {
        throw new IllegalArgumentException(error);
      }
    } else {
      reader = cReader;
      if (lReader.isStopped() || cReader.isStopped()) {
        shouldBeClosed = true;
      } else {
        cReader.startFromBegining();
      }
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
    } else {
      LOG.debug("Checkpointed file is in local stream directory");
      initializeCurrentFileFromCheckpointLocalStream(fileName);
    }
  }

  public void initializeCurrentFile() throws IOException, InterruptedException {
    LOG.info("Initializing partition reader's current file");
    cReader.build();

    if (partitionCheckpoint != null) {
      lReader.build(LocalStreamCollectorReader.getBuildTimestamp(
          streamName, partitionId.getCollector(), partitionCheckpoint));
      initializeCurrentFileFromCheckpoint();
    } else if (startTime != null) {
      lReader.build(startTime);
      initializeCurrentFileFromTimeStamp(startTime);
    } else {
      startFromStartOfStream();
    }
    if (reader != null) {
      LOG.info("Intialized currentFile:" + reader.getCurrentFile() +
          " currentLineNum:" + reader.getCurrentLineNum());
    }
  }

  private void startFromStartOfStream()
      throws IOException, InterruptedException {

    Date startingFileTimeStamp = null;
    String startingFile = null;
    reader = lReader;

    FileStatus startingDir = getStartingDIrFromStream();

    if (startingDir != null) {
      Date startingDirTimeStamp = ((LocalStreamCollectorReader) reader).
          getDateFromPath(reader.getStreamDir(),
              startingDir);
      // listing from start of the stream
      lReader.build(startingDirTimeStamp);
      FileStatus startingFileStatus = reader.getFirstFileInStream();
      if (startingFileStatus != null) {
        startingFile = startingFileStatus.getPath().getName();
        String collectorFile = CollectorStreamReader.getCollectorFileName(
            streamName, startingFile);
        startingFileTimeStamp = CollectorStreamReader
            .getDateFromCollectorFile(collectorFile);
        initializeCurrentFileFromTimeStamp(startingFileTimeStamp);
      }
    }
    if ((startingDir == null) || (startingFile == null)) {
      reader = cReader;
      if (reader.isEmpty()) {
        // close the reader if stream is empty or
        // wait for next file creation
        reader.startFromBegining();
      } else {
        FileStatus startingFileStatus = reader.getFirstFileInStream();
        if (startingFileStatus != null) {
          startingFile = startingFileStatus.getPath().getName();
          startingFileTimeStamp = CollectorStreamReader.getDateFromCollectorFile(
              startingFile);
          initializeCurrentFileFromTimeStamp(startingFileTimeStamp);
        }
      }
    }
  }

  private FileStatus getStartingDIrFromStream() throws IOException {
    List<FileStatus> leastTimeStampFileStatus = new ArrayList<FileStatus>();
    lReader.getStartingDirFromStream(reader.getFileSystem(),
        reader.getStreamDir(), 0, leastTimeStampFileStatus);
    if (leastTimeStampFileStatus.size() > 0) {
      return leastTimeStampFileStatus.get(0);
    }
    return null;
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
        LOG.info("Switching to collector stream as we reached end of" +
            " stream on local stream");
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
          LOG.info("Did not find current file in local stream as well") ;
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
        LOG.info("Reading file " + reader.getCurrentFile() +
            " and lineNum:" + reader.getCurrentLineNum());
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
}
