package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.utils.SecureLoginUtil;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;

public abstract class AbstractMessagingDatabusConsumer 
    extends AbstractMessageConsumer 
    implements MessagingConsumerConfig {
  protected static final Log LOG = LogFactory.getLog(
      AbstractMessagingDatabusConsumer.class);
  protected static final long ONE_HOUR_IN_MILLIS = 1 * 60 * 60 * 1000;

  protected BlockingQueue<QueueEntry> buffer;

  protected final Map<PartitionId, PartitionReader> readers = 
      new HashMap<PartitionId, PartitionReader>();

  protected CheckpointProvider checkpointProvider;
  protected Checkpoint currentCheckpoint;
  protected long waitTimeForFileCreate;
  protected int bufferSize;
  protected DataEncodingType dataEncodingType;

  @Override
  protected void init(ClientConfig config) throws IOException {
    initializeConfig(config);
    start();
  }

  protected static CheckpointProvider createCheckpointProvider(
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

  protected void initializeConfig(ClientConfig config) throws IOException {
    super.init(config);
    // verify authentication
    if (UserGroupInformation.isSecurityEnabled()) {
      String principal = config.getString(consumerPrincipal);
      String keytab = config.getString(consumerKeytab);
      if (principal != null && keytab != null) {
        SecureLoginUtil.login(consumerPrincipal, principal,
            consumerKeytab, keytab);
      } else {
        LOG.info("There is no principal or key tab file passed. Using the" +
            " commandline authentication.");
      }
    }

    // Create checkpoint provider and initialize checkpoint
    String chkpointProviderClassName = config.getString(
        chkProviderConfig, DEFAULT_CHK_PROVIDER);
    String databusCheckpointDir = config.getString(checkpointDirConfig, 
        DEFAULT_CHECKPOINT_DIR);
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

    //create buffer
    bufferSize = config.getInteger(queueSizeConfig, DEFAULT_QUEUE_SIZE);
    buffer = new LinkedBlockingQueue<QueueEntry>(bufferSize);

    // initialize other common configuration
    waitTimeForFileCreate = config.getLong(waitTimeForFileCreateConfig,
        DEFAULT_WAIT_TIME_FOR_FILE_CREATE);
    dataEncodingType = DataEncodingType.valueOf(
        config.getString(dataEncodingConfg, DEFAULT_DATA_ENCODING));
    LOG.debug("Using data encoding type as " + dataEncodingType);
  }

  public Map<PartitionId, PartitionReader> getPartitionReaders() {
    return readers;
  }

  public Checkpoint getCurrentCheckpoint() {
    return currentCheckpoint;
  }

  @Override
  public synchronized Message next() throws InterruptedException {
    QueueEntry entry;
    entry = buffer.take();
    currentCheckpoint.set(entry.getPartitionId(), entry.getPartitionChkpoint());
    return entry.getMessage();
  }

  protected synchronized void start() throws IOException {
    createPartitionReaders();
    for (PartitionReader reader : readers.values()) {
      reader.start();
    }
  }

  protected abstract void createPartitionReaders() throws IOException;

  protected Date getPartitionTimestamp(PartitionId id, PartitionCheckpoint pck,
      Date allowedStartTime) {
    Date partitionTimestamp = startTime;
    if (startTime == null && pck == null) {
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
          + " checkpoint:" + pck);
    }

    return partitionTimestamp;
  }

  protected String getChkpointKey() {
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
