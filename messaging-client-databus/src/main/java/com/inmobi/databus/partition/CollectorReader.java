package com.inmobi.databus.partition;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2012 - 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
  //this is the compression factor found from compressing and uncompressing various files
  //and by averaging them out.
  private static final double COMPRESSION_FACTOR = 3.8;

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
      Date buildTimestamp = null;
      if (partitionCheckpoint != null) {
        buildTimestamp = LocalStreamCollectorReader.
            getBuildTimestamp(streamName, partitionId.getCollector(), partitionCheckpoint);
      } else if (startTime != null) {
        buildTimestamp = startTime;
      } else {
        buildTimestamp = null;
      }
      lReader.initializeBuildTimeStamp(buildTimestamp);

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
    boolean useCReader = false;
    if (!lReader.isEmpty()) {
      if (!lReader.initializeCurrentFile(new PartitionCheckpoint(
          DatabusStreamFile.create(streamName, localStreamFileName),
          partitionCheckpoint.getLineNum()))) {
        // could not initialize from checkpoint
        // for following reasons
        // 1. checkpointed file does not exist or stop time has reached
        // 1.1 there are more files higher than checkpointed file.
        // 1.2 there are no files higher than checkpointed file.
        // for case 1.2, we should switch to collector stream reader.
        if (!lReader.initFromNextHigher(localStreamFileName)) {
          useCReader = true;
        }
      }
    } else {
      useCReader = true;
    }

    if (useCReader) {
      reader = cReader;
      String collectorFileName = CollectorStreamReader.getCollectorFileName(
          streamName, localStreamFileName);
      cReader.startFromNextHigher(collectorFileName);
    } else {
      reader = lReader;
    }
  }

  private void initializeCurrentFileFromCheckpoint()
      throws IOException, InterruptedException {
    String fileName = partitionCheckpoint.getFileName();
    if (CollectorStreamReader.isCollectorFile(fileName)) {
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
      lReader.build();
      if (partitionCheckpoint != null) {
        initializeCurrentFileFromCheckpoint();
      } else if (startTime != null) {
        initializeCurrentFileFromTimeStamp(startTime);
      } else {
        initializeCurrentFileFromStartOfStream();
      }
    } else {
      reader = cReader;
      initializeCurrentFileFromCollectorStreamOnly();
    }
    if (reader != null) {
      LOG.info("Intialized currentFile:" + reader.getCurrentFile()
          + " currentLineNum:" + reader.getCurrentLineNum());
    }
  }

  private void initializeCurrentFileFromCollectorStreamOnly()
      throws IOException, InterruptedException {
    if (partitionCheckpoint != null) {
      if (!reader.initializeCurrentFile(partitionCheckpoint)) {
        cReader.startFromNextHigher(partitionCheckpoint.getFileName());
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
        reader.updateLatestMinuteAlreadyReadForCollectorReader();
        line = super.readLine();
      } else {
        return null;
      }
    }
    return line;
  }

  @Override
  public MessageCheckpoint getMessageCheckpoint() {
    if (reader != null && getCurrentFile() != null) {
      return new PartitionCheckpoint(reader.getCurrentStreamFile(),
          reader.getCurrentLineNum());
    }
    return null;
  }

  @Override
  public MessageCheckpoint buildStartPartitionCheckpoints() {
    return null;
  }

  @Override
  public synchronized Long getReaderBackLog() throws IOException {
    Long collectorStreamPendingSize = 0L, localStreamPendingSize = 0L;
    int retryCount = 0;
    int maxRetryThreshold = 10;
    while (cReader.getCurrentFile() == null && retryCount < maxRetryThreshold) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.info("Sleep Interrupted while waiting for collector reader initialization. Ignoring the interrupt");
      }
      retryCount++;
    }
    //get collector reader remaining size
    if (cReader.getCurrentFile() != null) {
      collectorStreamPendingSize += cReader.getPendingSize(cReader.getCurrentFile());
      LOG.info("Pending Size of uncompressed data inside collector output location starting from collector file: " +
              cReader.getCurrentFile() + " is " + collectorStreamPendingSize);
    }
    //get local reader remaining size
    retryCount = 0;
    while (lReader.getCurrentFile() == null && retryCount < maxRetryThreshold) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.info("Sleep Interrupted while waiting for local stream reader initialization. Ignoring the interrupt");
      }
      retryCount++;
    }
    if (lReader.getCurrentFile() != null) {
      localStreamPendingSize += lReader.getPendingSize();
      LOG.info("Pending Size of compressed data inside streams_local output location starting from file: " +
              lReader.getCurrentFile() + " is " + localStreamPendingSize);
    }
    LOG.info("Pending Size of uncompressed data inside collector reader " + collectorStreamPendingSize
            + " Pending Size inside local stream reader " + localStreamPendingSize
            + " Total pending size " + (collectorStreamPendingSize + localStreamPendingSize));
    long localStreamPendingSizeAdjusted =  (long) ((double) localStreamPendingSize * COMPRESSION_FACTOR);
    LOG.info("local stream size of pending data compressed "+localStreamPendingSize +" local stream size of pending data adjusted with " +
            "compression factor of "+ COMPRESSION_FACTOR + " is "+localStreamPendingSizeAdjusted);
    return (collectorStreamPendingSize + localStreamPendingSizeAdjusted);
  }
}
