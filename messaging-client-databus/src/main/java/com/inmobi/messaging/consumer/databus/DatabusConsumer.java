package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.SourceStream;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;

/**
 * Consumes data from the configured databus stream (databus.stream) for each 
 * consumer (databus.consumer).
 * 
 * Max consumer buffer size is configurable via databus.consumer.buffer.size. 
 * The default value is 1000.
 *
 * Initializes partition readers for each active collector on the stream.
 * TODO: Dynamically detect if new collectors are added and start readers for
 *  them 
 */
public class DatabusConsumer extends AbstractMessageConsumer {
  private static final Log LOG = LogFactory.getLog(DatabusConsumer.class);


  public static final String DEFAULT_CHK_PROVIDER = FSCheckpointProvider.class
      .getName();
  public static final int DEFAULT_QUEUE_SIZE = 1000;

  private DatabusConfig databusConfig;
  private String streamName;
  private String consumerName;
  private BlockingQueue<QueueEntry> buffer;

  private final Map<PartitionId, PartitionReader> readers = 
      new HashMap<PartitionId, PartitionReader>();

  private CheckpointProvider checkpointProvider;
  private Checkpoint currentCheckpoint;

  @Override
  protected void init(ClientConfig config) {
    super.init(config);
    this.streamName = config.getString("databus.stream");
    this.consumerName = config.getString("databus.consumer");
    int queueSize = config.getInteger("databus.consumer.buffer.size",
                      DEFAULT_QUEUE_SIZE);
    buffer = new LinkedBlockingQueue<QueueEntry>(queueSize);
    
    this.checkpointProvider = new FSCheckpointProvider(".");

    try {
      byte[] chkpointData = checkpointProvider.read(getChkpointKey());
      if (chkpointData != null) {
        this.currentCheckpoint = new Checkpoint(chkpointData);
      }
      DatabusConfigParser parser = new DatabusConfigParser(null);
      databusConfig = parser.getConfig();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    LOG.info("Databus consumer initialized with streamName:" + streamName +
            " consumerName:" + consumerName + " queueSize:" + queueSize +
            " checkPoint:" + currentCheckpoint);
    start();
  }

  @Override
  public synchronized Message next() {
    QueueEntry entry;
    try {
      entry = buffer.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    currentCheckpoint.set(entry.partitionId, entry.partitionChkpoint);
    return entry.message;
  }

  private synchronized void start() {
    if (currentCheckpoint == null) {
      initializeCheckpoint();
    }

    // start partition readers for each checkpoint.
    for (Map.Entry<PartitionId, PartitionCheckpoint> cpEntry : currentCheckpoint
        .getPartitionsCheckpoint().entrySet()) {
      PartitionReader reader = new PartitionReader(cpEntry.getKey(),
          cpEntry.getValue(), databusConfig, buffer, streamName);
      readers.put(cpEntry.getKey(), reader);
      LOG.info("Starting partition reader " + cpEntry.getKey());
      reader.start();
    }
  }

  private void initializeCheckpoint() {
    Map<PartitionId, PartitionCheckpoint> partitionsChkPoints = 
        new HashMap<PartitionId, PartitionCheckpoint>();
    this.currentCheckpoint = new Checkpoint(partitionsChkPoints);
    SourceStream sourceStream = databusConfig.getSourceStreams().get(streamName);
    LOG.debug("Stream name: " + sourceStream.getName());
    System.out.println("stream name: " + sourceStream.getName());
    for (String c : sourceStream.getSourceClusters()) {
      Cluster cluster = databusConfig.getClusters().get(c);
      try {
        FileSystem fs = FileSystem.get(cluster.getHadoopConf());
        Path path = new Path(cluster.getDataDir(), streamName);
        LOG.debug("Stream dir: " + path);
        System.out.println("Stream dir: " + path);
        FileStatus[] list = fs.listStatus(path);
        if (list == null || list.length == 0) {
          LOG.warn("No collector dirs available in stream directory");
          return;
        }
        for (FileStatus status : list) {
          String collector = status.getPath().getName();
          LOG.debug("Collector is " + collector);
          System.out.println("Collector is " + collector);
          PartitionId id = new PartitionId(cluster.getName(), collector);
          partitionsChkPoints.put(id, new PartitionCheckpoint(null, -1));
          LOG.info("Created partition " + id);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  private String getChkpointKey() {
    return consumerName + "_" + streamName;
  }

  @Override
  public synchronized void reset() {
    // restart the service, consumer will start streaming from the last saved
    // checkpoint
    close();
    try {
      this.currentCheckpoint = new Checkpoint(
          checkpointProvider.read(getChkpointKey()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    start();
  }

  @Override
  public synchronized void mark() {
    try {
      checkpointProvider.checkpoint(getChkpointKey(),
          currentCheckpoint.toBytes());
      LOG.info("Committed checkpoint:" + currentCheckpoint);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void close() {
    for (PartitionReader reader : readers.values()) {
      reader.close();
    }
    checkpointProvider.close();
  }

  @Override
  public boolean isMarkSupported() {
	return true;
  }

}
