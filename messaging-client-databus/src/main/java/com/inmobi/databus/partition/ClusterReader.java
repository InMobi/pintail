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
    leastPartitionCheckpoint = ((DatabusStreamWaitingReader)reader)
        .getLeastCheckpoint();
    Date buildTimestamp = null;
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

  public void initializeCurrentFile() throws IOException, InterruptedException {
    LOG.info("Initializing partition reader's current file");

    // Build the reader from buildTimestamp
    reader.build();

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
