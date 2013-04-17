package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.utils.SecureLoginUtil;
import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;
import com.inmobi.messaging.metrics.DatabusConsumerStatsExposer;

public abstract class AbstractMessagingDatabusConsumer 
    extends AbstractMessageConsumer implements MessagingConsumerConfig {
  protected static final Log LOG = LogFactory.getLog(
      AbstractMessagingDatabusConsumer.class);
  protected static final long ONE_MINUTE_IN_MILLIS = 1 * 60 * 1000;

  protected BlockingQueue<QueueEntry> buffer;

  protected final Map<PartitionId, PartitionReader> readers = 
      new HashMap<PartitionId, PartitionReader>();

  protected CheckpointProvider checkpointProvider;
  protected ConsumerCheckpoint currentCheckpoint;
  protected long waitTimeForFileCreate;
  protected int bufferSize;
  protected String retentionInHours;
  protected int consumerNumber;
  protected int totalConsumers;
  protected Set<Integer> partitionMinList;
  protected String relativeStartTimeStr;

  @Override
  protected void init(ClientConfig config) throws IOException {
    try {
      initializeConfig(config);
      start();
    } catch (Throwable th) {
      close();
      throw new IllegalArgumentException(th);
    }
  }

  public static CheckpointProvider createCheckpointProvider(
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

    // Read consumer id
    String consumerIdStr = config.getString(consumerIdInGroupConfig,
        DEFAULT_CONSUMER_ID);
    String[] id = consumerIdStr.split("/");
    try {
      consumerNumber = Integer.parseInt(id[0]);
      totalConsumers = Integer.parseInt(id[1]);
      partitionMinList = new HashSet<Integer>();
      if (isValidConfiguration()) {
        for (int i = 0; i < 60; i++) {
          if ((i % totalConsumers) == (consumerNumber - 1)) {
            partitionMinList.add(i);
          }
        }
      } else {
        throw new IllegalArgumentException("Invalid consumer group membership");
      }
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Invalid consumer group membership",
          nfe);
    }
    // Create checkpoint provider and initialize checkpoint
    String chkpointProviderClassName = config.getString(
        chkProviderConfig, DEFAULT_CHK_PROVIDER);
    String databusCheckpointDir = config.getString(checkpointDirConfig, 
        DEFAULT_CHECKPOINT_DIR);
    this.checkpointProvider = createCheckpointProvider(
        chkpointProviderClassName, databusCheckpointDir);

    createCheckpoint();
    currentCheckpoint.read(checkpointProvider, getChkpointKey());
    
    //create buffer
    bufferSize = config.getInteger(queueSizeConfig, DEFAULT_QUEUE_SIZE);
    buffer = new LinkedBlockingQueue<QueueEntry>(bufferSize);

    // initialize other common configuration
    waitTimeForFileCreate = config.getLong(waitTimeForFileCreateConfig,
        DEFAULT_WAIT_TIME_FOR_FILE_CREATE);

    // get the retention period of the topic
    retentionInHours = config.getString(retentionConfig);

    relativeStartTimeStr = config.getString(relativeStartTimeConfig);

    if (relativeStartTimeStr == null && retentionInHours!= null) {
      LOG.warn("retentionConfig is deprecated");
      int minutes = (Integer.parseInt(retentionInHours)) * 60;
      relativeStartTimeStr = String.valueOf(minutes);
    }

  }

  protected boolean isValidConfiguration() {
    if (consumerNumber > 0 && totalConsumers > 0) {
      if (consumerNumber <= totalConsumers) {
        return true;
      } 
    } 
    return false;
  }

  public Map<PartitionId, PartitionReader> getPartitionReaders() {
    return readers;
  }

  protected abstract void createCheckpoint();

  public Set<Integer> getPartitionMinList() {
    return partitionMinList;
  }

  public ConsumerCheckpoint getCurrentCheckpoint() {
    return currentCheckpoint;
  }
  

  @Override
  protected Message getNext() throws InterruptedException {
    QueueEntry entry;
    entry = buffer.take();
    setMessageCheckpoint(entry);
    return entry.getMessage();
  }

  private void setMessageCheckpoint(QueueEntry entry) {
    MessageCheckpoint msgchk = entry.getMessageChkpoint();
    currentCheckpoint.set(entry.getPartitionId(), msgchk);
  }
  
  @Override
  protected Message getNext(long timeout, TimeUnit timeunit) 
      throws InterruptedException {
    QueueEntry entry;
    entry = buffer.poll(timeout, timeunit);
    if (entry == null) {
      return null;
    }
    setMessageCheckpoint(entry);
    return entry.getMessage();
  }

  protected synchronized void start() throws Exception {
    createPartitionReaders();
    for (PartitionReader reader : readers.values()) {
      reader.start();
    }
  }

  protected abstract void createPartitionReaders() throws Exception;

  protected Date getPartitionTimestamp(PartitionId id, MessageCheckpoint pck)
      throws Exception {
    Date partitionTimestamp = null;
    if (isCheckpointExists(pck)) {
      LOG.info("Checkpoint exists..Starting from the checkpoint");
      partitionTimestamp = null;
    } else if (relativeStartTimeStr != null) {
      partitionTimestamp = findStartTimeStamp(relativeStartTimeStr);
      LOG.info("checkpoint does not exists and relative start time is provided" +
          "started from relative start time" + partitionTimestamp);
    } else if (startTime != null) {
      partitionTimestamp = startTime;
      LOG.info("there is no checkpoint and no relative start time is provided" +
          "starting from absolute start time" + partitionTimestamp);
    } else {
      throw new Exception("please provide at least one of the mandary " +
          "start-up options in the configuration to start the consumer: "
          + "1. provide a checkpoint dir path which has the existing checkpoint"+
          " 2. relative start time " + "3. absolute start time");
    }
    return partitionTimestamp;
  }

  protected String getChkpointKey() {
    return consumerName + "_" + topicName;
  }

  public boolean isCheckpointExists(MessageCheckpoint pck) {
    return !(pck == null || pck.isNULL());
  }

  protected Date findStartTimeStamp(String relativeStartTimeStr) {
    long currentMillis = System.currentTimeMillis();
    long relativeStartTime = Long.parseLong(relativeStartTimeStr);
    return new Date(currentMillis -
        (relativeStartTime * ONE_MINUTE_IN_MILLIS));
  }

  @Override
  protected void doReset() throws Exception {
    // restart the service, consumer will start streaming from the last saved
    // checkpoint
    close();
    currentCheckpoint.read(checkpointProvider, getChkpointKey());
    LOG.info("Resetting to checkpoint:" + currentCheckpoint);
    buffer = new LinkedBlockingQueue<QueueEntry>(bufferSize);
    start();
  }

  @Override
  protected void doMark() throws IOException {
    currentCheckpoint.write(checkpointProvider, getChkpointKey());
    LOG.info("Committed checkpoint:" + currentCheckpoint);
  }

  @Override
  public synchronized void close() {
    for (PartitionReader reader : readers.values()) {
      reader.close();
      removeStatsExposer(reader.getStatsExposer());
    }
    readers.clear();
    if (buffer != null) {
      buffer.clear();
    }
    super.close();
  }

  @Override
  public boolean isMarkSupported() {
    return true;
  }

  @Override
  protected AbstractMessagingClientStatsExposer getMetricsImpl() {
    return new DatabusConsumerStatsExposer(topicName, consumerName,
        consumerNumber);
  }
}