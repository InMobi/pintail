package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
 * Consumes data from the configured databus stream topic. 
 * 
 * Initializes the databus configuration from the configuration file specified
 * by the configuration {@value #databusConfigFileKey}, the default value is
 * {@value #DEFAULT_DATABUS_CONFIG_FILE} 
 *
 * Consumer can specify a comma separated list of clusters from which the stream
 * should be streamed via configuration {@value #databusClustersConfig}. If no
 * such configuration exists, it will stream from all the source clusters of the 
 * stream.
 *  
 * This consumer supports mark and reset. Whenever user calls mark, the current
 * consumption will be check-pointed in a directory configurable via 
 * {@value #checkpointDirConfig}. The default value for value for checkpoint
 * directory is <code>.</code>. After reset(), consumer will start reading
 * messages from last check-pointed position.
 * 
 * Maximum consumer buffer size is configurable via {@value #queueSizeConfig}. 
 * The default value is {@value #DEFAULT_QUEUE_SIZE}.
 * 
 * If consumer is reading from the file that is currently being written by
 * producer, consumer will wait for flush to happen on the file. The wait time
 * for flush is configurable via {@value #waitTimeForFlushConfig}, and default
 * value is {@value #DEFAULT_WAIT_TIME_FOR_FLUSH}
 *
 * Initializes partition readers for each active collector on the stream.
 * TODO: Dynamically detect if new collectors are added and start readers for
 *  them 
 */
public class DatabusConsumer extends AbstractMessageConsumer {
  private static final Log LOG = LogFactory.getLog(DatabusConsumer.class);


  public static final String DEFAULT_CHK_PROVIDER = FSCheckpointProvider.class
      .getName();
  public static final int DEFAULT_QUEUE_SIZE = 5000;
  public static final long DEFAULT_WAIT_TIME_FOR_FLUSH = 1000; // 1 second
  public static final String DEFAULT_DATABUS_CONFIG_FILE = "databus.xml";
  
  public static final String queueSizeConfig = "databus.consumer.buffer.size";
  public static final String waitTimeForFlushConfig = 
      "databus.consumer.waittime.forcollectorflush";
  public static final String checkpointDirConfig = 
      "databus.consumer.checkpoint.dir";
  public static final String databusConfigFileKey = "databus.conf";
  public static final String databusClustersConfig = "databus.consumer.clusters";
  
  private static final long ONE_DAY_IN_MILLIS = 1 * 24 * 60 * 60 * 1000;

  private DatabusConfig databusConfig;
  private BlockingQueue<QueueEntry> buffer;
  private String databusCheckpointDir;

  private final Map<PartitionId, PartitionReader> readers = 
      new HashMap<PartitionId, PartitionReader>();

  private CheckpointProvider checkpointProvider;
  private Checkpoint currentCheckpoint;
  private long waitTimeForFlush;
  private int bufferSize;
  private String[] clusters;

  @Override
  protected void init(ClientConfig config) {
    super.init(config);
    initializeConfig(config);
    start();
  }

  void initializeConfig(ClientConfig config) {
    bufferSize = config.getInteger(queueSizeConfig,
        DEFAULT_QUEUE_SIZE);
    buffer = new LinkedBlockingQueue<QueueEntry>(bufferSize);
    databusCheckpointDir = config.getString(checkpointDirConfig, ".");
    waitTimeForFlush = config.getLong(waitTimeForFlushConfig,
        DEFAULT_WAIT_TIME_FOR_FLUSH);
    
    String clusterStr = config.getString(databusClustersConfig);
    if (clusterStr != null) {
      clusters = clusterStr.split(",");
    }
    this.checkpointProvider = new FSCheckpointProvider(databusCheckpointDir);

    try {
      byte[] chkpointData = checkpointProvider.read(getChkpointKey());
      if (chkpointData != null) {
        this.currentCheckpoint = new Checkpoint(chkpointData);
      } else {
        Map<PartitionId, PartitionCheckpoint> partitionsChkPoints = 
            new HashMap<PartitionId, PartitionCheckpoint>();
        this.currentCheckpoint = new Checkpoint(partitionsChkPoints);
      }
      String fileName = config.getString(databusConfigFileKey,
          DEFAULT_DATABUS_CONFIG_FILE);
      DatabusConfigParser parser = new DatabusConfigParser(fileName);
      databusConfig = parser.getConfig();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    LOG.info("Databus consumer initialized with streamName:" + topicName +
        " consumerName:" + consumerName + " startTime:" + startTime +
        " queueSize:" + bufferSize + " checkPoint:" + currentCheckpoint);
  }

  Map<PartitionId, PartitionReader> getPartitionReaders() {
    return readers;
  }

  Checkpoint getCurrentCheckpoint() {
    return currentCheckpoint;
  }

  DatabusConfig getDatabusConfig() {
    return databusConfig;
  }

  CheckpointProvider getCheckpointProvider() {
    return checkpointProvider; 
  }

  int getBufferSize() {
    return bufferSize;
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
    createPartitionReaders();
    for (PartitionReader reader : readers.values()) {
      reader.start();
    }
  }

  private void createPartitionReaders() {
    Map<PartitionId, PartitionCheckpoint> partitionsChkPoints = 
        currentCheckpoint.getPartitionsCheckpoint();
    if (!databusConfig.getSourceStreams().containsKey(topicName)) {
      throw new RuntimeException("Stream " + topicName + " does not exist");
    }
    SourceStream sourceStream = databusConfig.getSourceStreams().get(topicName);
    LOG.debug("Stream name: " + sourceStream.getName());
    Set<String> clusterNames;
    if (clusters != null) {
      clusterNames = new HashSet<String>();
      for (String c : clusters) {
        if (sourceStream.getSourceClusters().contains(c)) {
          clusterNames.add(c);
        }
      }
    } else {
      clusterNames = sourceStream.getSourceClusters();
    }
    long currentMillis = System.currentTimeMillis();
    for (String c : clusterNames) {
      LOG.debug("Creating partition readers for cluster:" + c);
      Cluster cluster = databusConfig.getClusters().get(c);
      long retentionMillis = 
          sourceStream.getRetentionInDays(c) * ONE_DAY_IN_MILLIS;
      Date allowedStartTime = new Date(
           currentMillis- retentionMillis);
      try {
        FileSystem fs = FileSystem.get(cluster.getHadoopConf());
        Path path = new Path(cluster.getDataDir(), topicName);
        LOG.debug("Stream dir: " + path);
        FileStatus[] list = fs.listStatus(path);
        if (list == null || list.length == 0) {
          LOG.warn("No collector dirs available in stream directory");
          return;
        }
        for (FileStatus status : list) {
          String collector = status.getPath().getName();
          LOG.debug("Collector is " + collector);
          PartitionId id = new PartitionId(cluster.getName(), collector);
          if (partitionsChkPoints.get(id) == null) {
            partitionsChkPoints.put(id, null);
          }
          Date partitionTimestamp = startTime;
          if (startTime == null && partitionsChkPoints.get(id) == null) {
            LOG.info("There is no startTime passed and no checkpoint exists" +
              " for the partition: " + id + " starting from the start" +
              " of the stream.");
            partitionTimestamp = allowedStartTime;
          } else if (startTime != null && startTime.before(allowedStartTime)) {
            LOG.info("Start time passed is before the start of the stream," +
                " starting from the start of the stream.");
            partitionTimestamp = allowedStartTime;
          } else {
            LOG.info("Creating partition with timestamp: " + partitionTimestamp
                + " checkpoint:" + partitionsChkPoints.get(id));
          }
          PartitionReader reader = new PartitionReader(id,
              partitionsChkPoints.get(id), cluster, buffer, topicName,
              partitionTimestamp, waitTimeForFlush);
          readers.put(id, reader);
          LOG.info("Created partition " + id);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private String getChkpointKey() {
    return consumerName + "_" + topicName;
  }

  @Override
  public synchronized void reset() {
    // restart the service, consumer will start streaming from the last saved
    // checkpoint
    close();
    try {
      this.currentCheckpoint = new Checkpoint(
          checkpointProvider.read(getChkpointKey()));
      LOG.info("Resetting to checkpoint:" + currentCheckpoint);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // reset to last marked position, ignore start time
    startTime = null;
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
    readers.clear();
    buffer.clear();
    buffer = new LinkedBlockingQueue<QueueEntry>(bufferSize);
  }

  @Override
  public boolean isMarkSupported() {
    return true;
  }

}
