package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.utils.SecureLoginUtil;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;

/**
 * Consumes data from the configured databus stream topic. 
 * 
 * Initializes the databus configuration from the configuration file specified
 * by the configuration {@value DatabusConsumerConfig#databusConfigFileKey},
 * the default value is
 * {@value DatabusConsumerConfig#DEFAULT_DATABUS_CONFIG_FILE} 
 *
 * Consumer can specify a comma separated list of clusters from which the stream
 * should be streamed via configuration 
 * {@value DatabusConsumerConfig#databusClustersConfig}. If no
 * such configuration exists, it will stream from all the source clusters of the 
 * stream.
 *  
 * This consumer supports mark and reset. Whenever user calls mark, the current
 * consumption will be check-pointed in a directory configurable via 
 * {@value DatabusConsumerConfig#checkpointDirConfig}. The default value for
 * value for checkpoint
 * directory is {@value DatabusConsumerConfig#DEFAULT_CHECKPOINT_DIR}. After
 * reset(), consumer will start reading
 * messages from last check-pointed position.
 * 
 * Maximum consumer buffer size is configurable via 
 * {@value DatabusConsumerConfig#queueSizeConfig}. 
 * The default value is {@value DatabusConsumerConfig#DEFAULT_QUEUE_SIZE}.
 * 
 * If consumer is reading from the file that is currently being written by
 * producer, consumer will wait for flush to happen on the file. The wait time
 * for flush is configurable via 
 * {@value DatabusConsumerConfig#waitTimeForFlushConfig}, and default
 * value is {@value DatabusConsumerConfig#DEFAULT_WAIT_TIME_FOR_FLUSH}
 *
 * Initializes partition readers for each active collector on the stream.
 * TODO: Dynamically detect if new collectors are added and start readers for
 *  them 
 */
public class DatabusConsumer extends AbstractMessageConsumer 
implements DatabusConsumerConfig {
  private static final Log LOG = LogFactory.getLog(DatabusConsumer.class);

  private static final long ONE_HOUR_IN_MILLIS = 1 * 60 * 60 * 1000;

  private DatabusConfig databusConfig;
  private BlockingQueue<QueueEntry> buffer;

  private final Map<PartitionId, PartitionReader> readers = 
      new HashMap<PartitionId, PartitionReader>();

  private CheckpointProvider checkpointProvider;
  private Checkpoint currentCheckpoint;
  private long waitTimeForFlush;
  private long waitTimeForFileCreate;
  private int bufferSize;
  private String[] clusters;
  private StreamType streamType;

  @Override
  protected void init(ClientConfig config) throws IOException {
    initializeConfig(config);
    start();
  }

  private static CheckpointProvider createCheckpointProvider(
      String checkpointProviderClassName, String chkpointDir) {
    CheckpointProvider chkProvider = null;
    try {
      Class<?> clazz = Class.forName(checkpointProviderClassName);
      Constructor<?> constructor = clazz.getConstructor(String.class);
      chkProvider = (CheckpointProvider) constructor.newInstance(new Object[]
          {chkpointDir});
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not create checkpoint provider "
          + checkpointProviderClassName, e);
    }
    return chkProvider;
  }

  void initializeConfig(ClientConfig config) throws IOException {
    super.init(config);
    bufferSize = config.getInteger(queueSizeConfig, DEFAULT_QUEUE_SIZE);
    buffer = new LinkedBlockingQueue<QueueEntry>(bufferSize);
    String databusCheckpointDir = config.getString(checkpointDirConfig, 
        DEFAULT_CHECKPOINT_DIR);
    waitTimeForFlush = config.getLong(waitTimeForFlushConfig,
        DEFAULT_WAIT_TIME_FOR_FLUSH);
    waitTimeForFileCreate = config.getLong(waitTimeForFileCreateConfig,
        DEFAULT_WAIT_TIME_FOR_FILE_CREATE);

    String clusterStr = config.getString(databusClustersConfig);
    if (clusterStr != null) {
      clusters = clusterStr.split(",");
    }

    String chkpointProviderClassName = config.getString(
        databusChkProviderConfig, DEFAULT_CHK_PROVIDER);
    this.checkpointProvider = createCheckpointProvider(
        chkpointProviderClassName, databusCheckpointDir);

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
    try {
      DatabusConfigParser parser = new DatabusConfigParser(fileName);
      databusConfig = parser.getConfig();
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not load databusConfig", e);
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      String principal = config.getString(databusConsumerPrincipal);
      String keytab = config.getString(databusConsumerKeytab);
      if (principal != null && keytab != null) {
        SecureLoginUtil.login(databusConsumerPrincipal, principal,
            databusConsumerKeytab, keytab);
      } else {
        LOG.info("There is no principal or key tab file passed. Using the" +
            " commandline authentication.");
      }
    }
    String type = config.getString(databusStreamType, DEFAULT_STREAM_TYPE);
    streamType = StreamType.valueOf(type);
    LOG.info("Databus consumer initialized with streamName:" + topicName +
        " consumerName:" + consumerName + " startTime:" + startTime +
        " queueSize:" + bufferSize + " checkPoint:" + currentCheckpoint +
        " streamType:" + streamType);
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
  public synchronized Message next() throws InterruptedException {
    QueueEntry entry;
    try {
      entry = buffer.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    currentCheckpoint.set(entry.getPartitionId(), entry.getPartitionChkpoint());
    return entry.getMessage();
  }

  private synchronized void start() throws IOException {
    createPartitionReaders();
    for (PartitionReader reader : readers.values()) {
      reader.start();
    }
  }

  private Set<String> getClusters(SourceStream sourceStream) {
    Set<String> clusterNames = new HashSet<String>();
    if (clusters != null) {
      if (streamType.equals(StreamType.MERGED)) {
        if (clusters.length > 1) {
          throw new IllegalArgumentException("More than one cluster configured"
              + " for " + streamType.name());
        }
      }
      for (String c : clusters) {
        if (sourceStream.getSourceClusters().contains(c)) {
          clusterNames.add(c);
        }
      }
    } else {
      if (streamType.equals(StreamType.LOCAL) || 
          streamType.equals(StreamType.COLLECTOR)) {
        clusterNames = sourceStream.getSourceClusters();
      } else if (streamType.equals(StreamType.MERGED)) {
        String mergeDestination = databusConfig
            .getPrimaryClusterForDestinationStream(sourceStream.getName())
            .getName();
        LOG.info("Merge destination:" + mergeDestination);
        clusterNames.add(mergeDestination);
      } else {
        LOG.info("No op for:" + streamType);
      }
    }
    return clusterNames;
  }

  private Cluster getDatabusCluster(String clusterName) {
    Cluster cluster = databusConfig.getClusters().get(clusterName);
    if (cluster == null) {
      throw new IllegalArgumentException("No such cluster:" + clusterName);
    }
    return cluster;
  }

  private List<String> getCollectors(Cluster cluster) throws IOException {
    List<String> collectors = new ArrayList<String>();    
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    Path path = new Path(cluster.getDataDir(), topicName);
    LOG.debug("Stream dir: " + path);
    FileStatus[] list = fs.listStatus(path);
    if (list != null && list.length > 0) {
      for (FileStatus status : list) {
        collectors.add(status.getPath().getName());
      }
    } else {
      LOG.warn("No collector dirs available in " + path);
    }
    return collectors;
  }

  private void createPartitionReader(String collector,
      Cluster cluster, Date allowedStartTime,
      Map<PartitionId, PartitionCheckpoint> partitionsChkPoints)
          throws IOException {
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
    LOG.debug("Creating partition " + id);
    PartitionReader reader = new PartitionReader(id,
        partitionsChkPoints.get(id), cluster, buffer, topicName,
        partitionTimestamp, waitTimeForFlush, waitTimeForFileCreate,
        streamType.equals(StreamType.LOCAL));    
    LOG.debug("Created partition " + id);
    readers.put(id, reader);
  }

  private void createPartitionReaders() throws IOException {
    Map<PartitionId, PartitionCheckpoint> partitionsChkPoints = 
        currentCheckpoint.getPartitionsCheckpoint();
    if (!databusConfig.getSourceStreams().containsKey(topicName)) {
      throw new RuntimeException("Stream " + topicName + " does not exist");
    }
    SourceStream sourceStream = databusConfig.getSourceStreams().get(topicName);
    LOG.debug("Stream name: " + sourceStream.getName());

    Set<String> clusterNames = getClusters(sourceStream);

    long currentMillis = System.currentTimeMillis();
    for (String c : clusterNames) {
      LOG.debug("Creating partition readers for cluster:" + c);
      Cluster cluster = getDatabusCluster(c);
      long retentionMillis = sourceStream.getRetentionInHours(c)
          * ONE_HOUR_IN_MILLIS;
      Date allowedStartTime = new Date(currentMillis- retentionMillis);
      if (streamType.equals(StreamType.COLLECTOR)) {
        LOG.info("Creating partition readers for all the collectors");
        for (String collector : getCollectors(cluster)) {
          createPartitionReader(collector, cluster, allowedStartTime,
              partitionsChkPoints);
        }
      } else {
        LOG.info("Creating partition reader for cluster");
        createPartitionReader(null, cluster, allowedStartTime,
            partitionsChkPoints);
      }
    }
  }

  private String getChkpointKey() {
    return consumerName + "_" + topicName;
  }

  @Override
  public synchronized void reset() throws IOException {
    // restart the service, consumer will start streaming from the last saved
    // checkpoint
    close();
    this.currentCheckpoint = new Checkpoint(
        checkpointProvider.read(getChkpointKey()));
    LOG.info("Resetting to checkpoint:" + currentCheckpoint);
    // reset to last marked position, ignore start time
    startTime = null;
    start();
  }

  @Override
  public synchronized void mark() throws IOException {
    checkpointProvider.checkpoint(getChkpointKey(),
        currentCheckpoint.toBytes());
    LOG.info("Committed checkpoint:" + currentCheckpoint);
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
