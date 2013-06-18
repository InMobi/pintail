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
import com.inmobi.messaging.consumer.EndOfStreamException;
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
  protected Date stopTime;
  protected Boolean startOfStream;
  private int closedReadercount;

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
        LOG.info("There is no principal or key tab file passed. Using the"
            + " commandline authentication.");
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

    if (relativeStartTimeStr == null && retentionInHours != null) {
      LOG.warn(retentionConfig  + " is deprecated."
          + " Use " + relativeStartTimeConfig + " instead");
      int minutes = (Integer.parseInt(retentionInHours)) * 60;
      relativeStartTimeStr = String.valueOf(minutes);
    }

    String stopTimeStr = config.getString(stopDateConfig);
    stopTime = getDateFromString(stopTimeStr);

    startOfStream = config.getBoolean(startOfStreamConfig,
        DEFAULT_START_OF_STREAM);
    closedReadercount = 0;
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

  /**
   * @throws throws an EndOfStreamException When consumer consumed all messages
   *  till stopTime
   * @return Message if Message is available on the stream
   *         Otherwise waits for the Message to be available on the stream
   */
  @Override
  protected Message getNext()
      throws InterruptedException, EndOfStreamException {
    // check whether it consumed all messages till stopTime
    checkClosedReaders();
    QueueEntry entry = null;
    for (int i = 0; i < readers.size(); i++) {
      entry = buffer.take();
      if (entry.getMessage() instanceof Message) {
        break;
      } else { // if (entry.getMessage() instanceof EOFMessage)
        closedReadercount++;
        checkClosedReaders();
      }
    }
    setMessageCheckpoint(entry);
    return (Message) entry.getMessage();
  }

  private void setMessageCheckpoint(QueueEntry entry) {
    MessageCheckpoint msgchk = entry.getMessageChkpoint();
    currentCheckpoint.set(entry.getPartitionId(), msgchk);
  }

  /**
   * @throws throws an EndOfStreamException When consumer consumed all messages
   *  till stopTime
   * @return Message if Message is available on the stream
   *         Null if Message is not available on the stream for a given timeout
   */
  @Override
  protected Message getNext(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException {
    // check whether it consumed all messages till stopTime
    checkClosedReaders();
    QueueEntry entry = null;
    for (int i = 0; i < readers.size(); i++) {
      entry = buffer.poll(timeout, timeunit);
      if (entry == null) {
        return null;
      }
      if (entry.getMessage() instanceof Message) {
        break;
      } else { // if (entry.getMessage() instanceof EOFMessage)
        closedReadercount++;
        checkClosedReaders();
      }
    }
    setMessageCheckpoint(entry);
    return (Message) entry.getMessage();
  }

  private void checkClosedReaders() throws EndOfStreamException {
    if (closedReadercount == readers.size()) {
      throw new EndOfStreamException();
    }
  }

  protected synchronized void start() throws IOException {
    createPartitionReaders();
    for (PartitionReader reader : readers.values()) {
      reader.start();
    }
  }

  protected abstract void createPartitionReaders() throws IOException;

  protected Date getPartitionTimestamp(PartitionId id, MessageCheckpoint pck)
      throws IOException {
    Date partitionTimestamp = null;
    if (isCheckpointExists(pck)) {
      LOG.info("Checkpoint exists..Starting from the checkpoint");
      partitionTimestamp = null;
    } else if (relativeStartTimeStr != null) {
      partitionTimestamp = findStartTimeStamp(relativeStartTimeStr);
      LOG.info("Checkpoint does not exists and relative start time is provided."
          + " Started from relative start time " + partitionTimestamp);
    } else if (startTime != null) {
      partitionTimestamp = startTime;
      LOG.info("There is no checkpoint and no relative start time is provided."
          + " Starting from absolute start time " + partitionTimestamp);
    } else if (startOfStream) {
      LOG.info("Starting from start of the stream ");
    } else {
      throw new IllegalArgumentException("Invalid configuration to start"
          + " the consumer. " + "Provide a checkpoint or relative startTime"
          + " or absolute startTime or startOfStream ");
    }
    //check whether the given stop date is before/after the start time
    isValidStopDate(partitionTimestamp);
    return partitionTimestamp;
  }

  public void isValidStopDate(Date partitionTimestamp) {
    if (partitionTimestamp != null) {
      if (stopTime != null && stopTime.before(partitionTimestamp)) {
        throw new IllegalArgumentException("Provided stopTime is beyond"
            + " the startTime." + "Provide a valid stopTime");
      }
    }
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
    return new Date(currentMillis - (relativeStartTime * ONE_MINUTE_IN_MILLIS));
  }

  @Override
  protected void doReset() throws IOException {
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
