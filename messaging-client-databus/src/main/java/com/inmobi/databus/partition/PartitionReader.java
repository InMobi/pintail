package com.inmobi.databus.partition;

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
        ((CollectorReaderStatsExposer)prMetrics), noNewFiles, stopTime);
    // initialize cluster and its directories
    LOG.info("Partition reader initialized with partitionId:" + partitionId +
        " checkPoint:" + partitionCheckpoint +  
        " startTime:" + startTime + " stopTime:" + stopTime +
        " currentReader:" + reader);
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
    LOG.info("Partition reader initialized with partitionId:" + partitionId +
        " checkPoint:" + partitionCheckpointList +  
        " startTime:" + startTime + " stopTime:" + stopTime +
        " currentReader:" + reader);
  }

  private PartitionReader(PartitionId partitionId,
      MessageCheckpoint msgCheckpoint,
      BlockingQueue<QueueEntry> buffer, Date startTime,
      PartitionReaderStatsExposer prMetrics)
          throws IOException {
    if (startTime == null && msgCheckpoint == null) {
      String msg = "StartTime and checkpoint both" +
          " cannot be null in PartitionReader";
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    this.partitionId = partitionId;
    this.buffer = buffer;
    this.prMetrics = prMetrics;
  }

  public synchronized void start() {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        while (!stopped && !thread.isInterrupted()) {
          long startTime = System.currentTimeMillis();
          try {
            while (!stopped && !inited) {
              init();
            }
            LOG.info("Started streaming the data from reader:" + reader);
            execute();
            if (stopped || thread.isInterrupted())
              return;
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
    thread = new Thread(runnable, this.partitionId.toString());
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

  void execute() {
    assert (reader != null);
    try {
      boolean closeReader = false;
      /*
       * close the reader if there are no files present in the stream
       *  for a given stopTime
       */
      if (reader.shouldBeClosed()) {
        closeReader = true;
      } else {
        closeReader = !(reader.openStream());
      }
      if (!closeReader) {
        LOG.info("Reading file " + reader.getCurrentFile() + 
            " and lineNum:" + reader.getCurrentLineNum());
      }
      while (!stopped && !closeReader) {
        Message msg = reader.readLine();
        if (msg != null) {
          // add the data to queue
          MessageCheckpoint checkpoint = reader.getMessageCheckpoint();
          buffer.put(new QueueEntry(msg, partitionId, checkpoint));
          prMetrics.incrementMessagesAddedToBuffer();
        } else {
          closeReader = true;
        }
      }
      if (closeReader) {
        LOG.info("No stream to read");
        putEOFMessageInBuffer();
        // close the reader if reader's status is "closing"
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
    buffer.put(new QueueEntry(eofMessage, partitionId, null));
  }

  public PartitionReaderStatsExposer getStatsExposer() {
    return prMetrics;
  }
}
