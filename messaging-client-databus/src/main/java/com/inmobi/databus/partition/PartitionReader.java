package com.inmobi.databus.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.QueueEntry;

public class PartitionReader {

  private static final Log LOG = LogFactory.getLog(PartitionReader.class);

  private final PartitionId partitionId;
  private final BlockingQueue<QueueEntry> buffer;
  private PartitionStreamReader reader;

  private Thread thread;
  private volatile boolean stopped;
  private boolean inited = false;

  public PartitionReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, Cluster cluster,
      BlockingQueue<QueueEntry> buffer, String streamName,
      Date startTime, long waitTimeForFlush, boolean isLocal) throws IOException {
    this(partitionId, partitionCheckpoint, cluster, buffer, streamName,
        startTime, waitTimeForFlush, isLocal, false);
  }

  PartitionReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, Cluster cluster,
      BlockingQueue<QueueEntry> buffer, String streamName,
      Date startTime, long waitTimeForFlush, boolean isLocal, boolean noNewFiles)
          throws IOException {
    if (startTime == null && partitionCheckpoint == null) {
      String msg = "StartTime and checkpoint both" +
        " cannot be null in PartitionReader";
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    this.partitionId = partitionId;
    this.buffer = buffer;

    if (partitionId.getCollector() == null) {
      reader = new ClusterReader(partitionId, partitionCheckpoint, cluster,
          streamName, startTime, isLocal, noNewFiles);
    } else {
      reader = new CollectorReader(partitionId, partitionCheckpoint, cluster,
          streamName, startTime, waitTimeForFlush, noNewFiles);
    }
    // initialize cluster and its directories
    LOG.info("Partition reader initialized with partitionId:" + partitionId +
        " checkPoint:" + partitionCheckpoint +  
        " startTime:" + startTime +
        " currentReader:" + reader);
  }

  public synchronized void start() {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        while (!stopped && !thread.isInterrupted()) {
          long startTime = System.currentTimeMillis();
          try {
            while (!inited) {
              init();
            }
            LOG.info("Started streaming the data from reader:" + reader);
            execute();
            if (stopped || thread.isInterrupted())
              return;
          } catch (Throwable e) {
            LOG.warn("Error in run", e);
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
  }

  Path getCurrentFile() {
    return reader.getCurrentFile();
  }

  PartitionStreamReader getReader() {
    return reader;
  }

  void execute() {
    assert (reader != null);
    try {
      reader.openStream();
      LOG.info("Reading file " + reader.getCurrentFile() + 
          " and lineNum:" + reader.getCurrentLineNum());
      while (!stopped) {
        String line = reader.readLine();
        if (line != null) {
          // add the data to queue
          byte[] data = Base64.decodeBase64(line);
          buffer.put(new QueueEntry(new Message(
              ByteBuffer.wrap(data)), partitionId,
              new PartitionCheckpoint(reader.getCurrentFile().getName(),
                  reader.getCurrentLineNum())));
        } else {
          LOG.info("No stream to read");
          return;
        }
      }
    } catch (Throwable e) {
      LOG.warn("Error while reading stream", e);
    } finally {
      try {
        reader.close();
      } catch (Exception e) {
        LOG.warn("Error while closing stream", e);
      }
    }
  }

}
