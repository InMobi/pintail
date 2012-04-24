package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;

public class DatabusConsumer extends AbstractMessageConsumer {

  public static final String DEFAULT_CHK_PROVIDER = FSCheckpointProvider.class
      .getName();

  private DatabusConfig databusConfig;
  private String streamName;
  private String consumerName;
  private final BlockingQueue<QueueEntry> buffer = new LinkedBlockingQueue<QueueEntry>(
      1000);

  private final Map<PartitionId, PartitionReader> readers = new HashMap<PartitionId, PartitionReader>();

  private CheckpointProvider checkpointProvider;
  private Checkpoint currentCheckpoint;

  @Override
  protected void init(ClientConfig config) {
    super.init(config);
    this.streamName = config.getString("databus.stream");
    this.consumerName = config.getString("databus.consumer");
    this.checkpointProvider = new FSCheckpointProvider(".");

    try {
      byte[] chkpointData = checkpointProvider.read(getChkpointKey());
      if (chkpointData != null) {
        this.currentCheckpoint = new Checkpoint(chkpointData);
      }
      DatabusConfigParser parser =
          new DatabusConfigParser(null);
      databusConfig = parser.getConfig();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
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
    currentCheckpoint.set(entry.partitionChkpoint.getId(),
        entry.partitionChkpoint);
    return entry.message;
  }

  public synchronized void start() {
    if (currentCheckpoint == null) {
      Map<PartitionId, PartitionCheckpoint> partitionsChkPoints = new HashMap<PartitionId, PartitionCheckpoint>();
      this.currentCheckpoint = new Checkpoint(partitionsChkPoints);
      for (String c : databusConfig.getSourceStreams().get(streamName)
          .getSourceClusters()) {
        Cluster cluster = databusConfig.getClusters().get(c);
        try {
          // System.out.println("----here 11---");
          FileSystem fs = FileSystem.get(cluster.getHadoopConf());
          Path path = new Path(cluster.getDataDir(), streamName);
          System.out.println(path);
          FileStatus[] list = fs.listStatus(path);
          for (FileStatus status : list) {
            String collector = status.getPath().getName();
            System.out.println("collector is " + collector);
            PartitionId id = new PartitionId(cluster.getName(), collector);
            partitionsChkPoints.put(id, new PartitionCheckpoint(id, null, -1));
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    for (PartitionCheckpoint partition : currentCheckpoint
        .getPartitionsCheckpoint().values()) {
      PartitionReader reader = new PartitionReader(partition, databusConfig, buffer,
          streamName);
      readers.put(partition.getId(), reader);
      System.out.println("Starting partition reader " + partition.getId());
      reader.start();
    }
  }

  private String getChkpointKey() {
    return consumerName + "_" + streamName;
  }

  @Override
  public synchronized void rollback() {
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
  public synchronized void commit() {
    try {
      checkpointProvider.checkpoint(getChkpointKey(),
          currentCheckpoint.toBytes());
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

}
