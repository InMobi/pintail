package com.inmobi.databus.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamReader;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;

public class PartitionReader {

  private static final Log LOG = LogFactory.getLog(PartitionReader.class);

  private final PartitionId partitionId;
  private final BlockingQueue<QueueEntry> buffer;
  private PartitionStreamReader reader;

  private Thread thread;
  private volatile boolean stopped;
  private boolean inited = false;
  private final DataEncodingType dataEncoding;

  public PartitionReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, Cluster cluster,
      BlockingQueue<QueueEntry> buffer, String streamName,
      Date startTime, long waitTimeForFlush, long waitTimeForFileCreate,
      boolean isLocal, DataEncodingType dataEncoding) throws IOException {
    this(partitionId, partitionCheckpoint, cluster, buffer, streamName,
        startTime, waitTimeForFlush, waitTimeForFileCreate, isLocal,
        dataEncoding, false);
  }

  public PartitionReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, FileSystem fs,
      BlockingQueue<QueueEntry> buffer, String streamName,
      Path streamDir, Configuration conf,
      String inputFormatClass, Date startTime, long waitTimeForFileCreate,
      DataEncodingType dataEncoding)
          throws IOException {
    this(partitionId, partitionCheckpoint, fs, buffer, streamName,
        streamDir, conf, inputFormatClass, startTime, waitTimeForFileCreate,
        dataEncoding, false);
  }

  PartitionReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, Cluster cluster,
      BlockingQueue<QueueEntry> buffer, String streamName,
      Date startTime, long waitTimeForFlush, long waitTimeForFileCreate,
      boolean isLocal, DataEncodingType dataEncoding, boolean noNewFiles)
          throws IOException {
    this(partitionId, partitionCheckpoint, buffer, startTime, dataEncoding);
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());

    if (partitionId.getCollector() == null) {
      if (isLocal) {
        reader = new ClusterReader(partitionId, partitionCheckpoint,
            fs, streamName,
            DatabusStreamReader.getStreamsLocalDir(cluster, streamName), 
            cluster.getHadoopConf(),
            TextInputFormat.class.getCanonicalName(),
            startTime, waitTimeForFileCreate, true, noNewFiles);
      } else {
        reader = new ClusterReader(partitionId, partitionCheckpoint,
            fs, streamName,
            DatabusStreamReader.getStreamsDir(cluster, streamName),
            cluster.getHadoopConf(),
            TextInputFormat.class.getCanonicalName(),
            startTime, waitTimeForFileCreate, true, noNewFiles);        
      }
    } else {
      reader = new CollectorReader(partitionId, partitionCheckpoint, fs,
          streamName,
          CollectorStreamReader.getCollectorDir(cluster, streamName,
              partitionId.getCollector()),
         DatabusStreamReader.getStreamsLocalDir(cluster, streamName),
         cluster.getHadoopConf(),
         startTime, waitTimeForFlush, waitTimeForFileCreate, noNewFiles);
    }
    // initialize cluster and its directories
    LOG.info("Partition reader initialized with partitionId:" + partitionId +
        " checkPoint:" + partitionCheckpoint +  
        " startTime:" + startTime +
        " currentReader:" + reader);
  }

  PartitionReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint, FileSystem fs,
      BlockingQueue<QueueEntry> buffer, String streamName,
      Path streamDir, Configuration conf,
      String inputFormatClass, Date startTime, long waitTimeForFileCreate,
      DataEncodingType dataEncoding, boolean noNewFiles)
          throws IOException {
    this(partitionId, partitionCheckpoint, buffer, startTime, dataEncoding);
    reader = new ClusterReader(partitionId, partitionCheckpoint,
        fs, streamName, streamDir, conf, inputFormatClass,
        startTime, waitTimeForFileCreate, false, noNewFiles);
    // initialize cluster and its directories
    LOG.info("Partition reader initialized with partitionId:" + partitionId +
        " checkPoint:" + partitionCheckpoint +  
        " startTime:" + startTime +
        " currentReader:" + reader);
  }

  private PartitionReader(PartitionId partitionId,
      PartitionCheckpoint partitionCheckpoint,
      BlockingQueue<QueueEntry> buffer, Date startTime,
      DataEncodingType dataEncoding)
          throws IOException {
    if (startTime == null && partitionCheckpoint == null) {
      String msg = "StartTime and checkpoint both" +
          " cannot be null in PartitionReader";
      LOG.warn(msg);
      throw new IllegalArgumentException(msg);
    }
    this.partitionId = partitionId;
    this.buffer = buffer;
    this.dataEncoding = dataEncoding;
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

  StreamFile getCurrentFile() {
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
        byte[] line = reader.readLine();
        if (line != null) {
          // add the data to queue
          byte[] data;
          if (dataEncoding.equals(DataEncodingType.BASE64)) {
            data = Base64.decodeBase64(line);
          } else {
            data = line;
          }
          buffer.put(new QueueEntry(new Message(
              ByteBuffer.wrap(data)), partitionId,
              new PartitionCheckpoint(reader.getCurrentFile(),
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
        reader.closeStream();
      } catch (Exception e) {
        LOG.warn("Error while closing stream", e);
      }
    }
  }

}
