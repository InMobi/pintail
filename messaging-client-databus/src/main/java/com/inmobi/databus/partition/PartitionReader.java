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
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.files.StreamFile;
import com.inmobi.messaging.EOFMessage;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.MessageCheckpoint;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class PartitionReader {

  private static final Log LOG = LogFactory.getLog(PartitionReader.class);

  private final PartitionId partitionId;
  private final BlockingQueue<QueueEntry> buffer;
  private PartitionStreamReader reader;

  private Thread thread;
  private volatile boolean stopped;
  private boolean inited = false;
  private final PartitionReaderStatsExposer prMetrics;

  public PartitionReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, Configuration conf,
      FileSystem fs, Path collectorDataDir,
      Path streamsLocalDir, BlockingQueue<QueueEntry> buffer, String streamName,
      Date startTime, long waitTimeForFlush,
      long waitTimeForFileCreate, PartitionReaderStatsExposer prMetrics, Date stopTime)
          throws IOException {
    this(partitionId, partitionCheckpoint, conf, fs, collectorDataDir,
        streamsLocalDir, buffer, streamName, startTime,
        waitTimeForFlush, waitTimeForFileCreate, prMetrics, false, stopTime);
  }

  public PartitionReader(PartitionId partitionId,
      PartitionCheckpointList partitionCheckpointList, FileSystem fs,
      BlockingQueue<QueueEntry> buffer, Path streamDir,
      Configuration conf, String inputFormatClass,
      Date startTime, long waitTimeForFileCreate, boolean isDatabusData,
      PartitionReaderStatsExposer prMetrics, Set<Integer> partitionMinList,
      Date stopTime)
          throws IOException {
    this(partitionId, partitionCheckpointList, fs, buffer, streamDir,
        conf, inputFormatClass, startTime, waitTimeForFileCreate, isDatabusData,
        prMetrics, false, partitionMinList, stopTime);
  }

  PartitionReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, Configuration conf,
      FileSystem fs,
      Path collectorDataDir,
      Path streamLocalDir,
      BlockingQueue<QueueEntry> buffer, String streamName, Date startTime,
      long waitTimeForFlush, long waitTimeForFileCreate,
      PartitionReaderStatsExposer prMetrics, boolean noNewFiles, Date stopTime)
          throws IOException {
    this(partitionId, partitionCheckpoint, buffer, startTime, prMetrics);
    reader = new CollectorReader(partitionId, partitionCheckpoint, fs,
        streamName, collectorDataDir, streamLocalDir, conf,
        startTime, waitTimeForFlush, waitTimeForFileCreate,
        ((CollectorReaderStatsExposer) prMetrics), noNewFiles, stopTime);
    // initialize cluster and its directories
    LOG.info("Partition reader initialized with partitionId:" + partitionId
        + " checkPoint:" + partitionCheckpoint
        + " startTime:" + startTime + " stopTime:" + stopTime
        + " currentReader:" + reader);
  }

  PartitionReader(PartitionId partitionId,
      PartitionCheckpointList partitionCheckpointList, FileSystem fs,
      BlockingQueue<QueueEntry> buffer, Path streamDir,
      Configuration conf, String inputFormatClass,
      Date startTime, long waitTimeForFileCreate, boolean isDatabusData,
      PartitionReaderStatsExposer prMetrics, boolean noNewFiles,
      Set<Integer> partitionMinList, Date stopTime)
          throws IOException {
    this(partitionId, partitionCheckpointList, buffer, startTime, prMetrics);
    reader = new ClusterReader(partitionId, partitionCheckpointList,
        fs, streamDir, conf, inputFormatClass, startTime,
        waitTimeForFileCreate, isDatabusData, prMetrics, noNewFiles,
        partitionMinList, stopTime);
    // initialize cluster and its directories
    LOG.info("Partition reader initialized with partitionId:" + partitionId
        + " checkPoint:" + partitionCheckpointList
        + " startTime:" + startTime + " stopTime:" + stopTime
        + " currentReader:" + reader);
  }

  private PartitionReader(PartitionId partitionId,
      MessageCheckpoint msgCheckpoint,
      BlockingQueue<QueueEntry> buffer, Date startTime,
      PartitionReaderStatsExposer prMetrics)
          throws IOException {
    this.partitionId = partitionId;
    this.buffer = buffer;
    this.prMetrics = prMetrics;
  }

  public synchronized void start(String readerNameSuffix) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        while (!stopped && !thread.isInterrupted()) {
          long startTime = System.currentTimeMillis();
          try {
            // initialize the reader once, until init succeeds
            while (!stopped && !inited) {
              init();
            }
            LOG.info("Started streaming the data from reader:" + reader);
            execute();
            if (stopped || thread.isInterrupted()) {
              return;
            }
          } catch (Throwable e) {
            LOG.warn("Error in run", e);
            prMetrics.incrementHandledExceptions();
          }
          long finishTime = System.currentTimeMillis();
          LOG.debug("Execution took ms : " + (finishTime - startTime));
          try {
            long sleep = 1000;
            if (sleep > 0) {
              LOG.debug("Sleeping for " + sleep);
              Thread.sleep(sleep);
            }
          } catch (InterruptedException e) {
            LOG.warn("thread interrupted " + thread.getName(), e);
            return;
          }
        }
      }
    };
    thread = new Thread(runnable, this.partitionId.toString() + "_"
        + readerNameSuffix);
    LOG.info("Starting thread " + thread.getName());
    thread.start();
  }

  void init() throws IOException, InterruptedException {
    if (!inited) {
      reader.initializeCurrentFile();
      inited = true;
    }
  }

  public void close() {
    stopped = true;
    LOG.info(Thread.currentThread().getName() + " stopped [" + stopped + "]");
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        LOG.warn("Error closing current stream", e);
      }
    }
  }

  public void join() {
    if (thread != null) {
      thread.interrupt();
      try {
        thread.join();
      } catch (InterruptedException ie) {
        LOG.warn("thread join interrupted " + thread.getName(), ie);
        return;
      }
    }
  }

  StreamFile getCurrentFile() {
    return reader.getCurrentFile();
  }

  PartitionStreamReader getReader() {
    return reader;
  }

  public Long getReaderBackLog() throws IOException {
    return reader.getReaderBackLog();
  }

  /**
   * Execute reads messages from the stream and adds them to the consumer buffer,
   * until no more messages are present or any exception occurs while reading
   */
  void execute() {
    assert (reader != null);
    try {
      boolean closeReader = false;
      // Close the reader if it should closed 
      // or reader not able to open the stream
      // > when will reader wont be able to open the stream?
      // If the reader could not be initialized - because the stop time
      // has reached, then currentfile would be null and
      // open stream returns false
      if (!stopped) {
        closeReader = !(reader.openStream());
        if (!closeReader) {
          LOG.info("Reading file " + reader.getCurrentFile()
              + " and lineNum:" + reader.getCurrentLineNum());
        }
      }
      while (!stopped && !closeReader) {
        // read the message from the stream reader
        Message msg = reader.readLine();
        if (msg != null) {
          // add the data to queue
          MessageCheckpoint checkpoint = reader.getMessageCheckpoint();
          buffer.put(new QueueEntry(msg, partitionId, checkpoint));
          prMetrics.incrementMessagesAddedToBuffer();
        } else {
          // message will be null, if the all messages are read
          // in case of stop criteria defined
          closeReader = true;
        }
      }
      if (closeReader) {
        LOG.info("No stream to read");
        putEOFMessageInBuffer();
        close();
        return;
      }
    } catch (InterruptedException ie) {
      LOG.info("Interrupted while reading stream", ie);
    } catch (Throwable e) {
      LOG.warn("Error while reading stream", e);
      prMetrics.incrementHandledExceptions();
    } finally {
      try {
        reader.closeStream();
      } catch (Exception e) {
        LOG.warn("Error while closing stream", e);
        prMetrics.incrementHandledExceptions();
      }
    }
  }

  public void putEOFMessageInBuffer() throws InterruptedException {
    EOFMessage eofMessage = new EOFMessage();
    buffer.put(new QueueEntry(eofMessage, partitionId,
        reader.getMessageCheckpoint()));
  }

  public PartitionReaderStatsExposer getStatsExposer() {
    return prMetrics;
  }

  public MessageCheckpoint buildStartPartitionCheckpoints() {
    return reader.buildStartPartitionCheckpoints();
  }
}
