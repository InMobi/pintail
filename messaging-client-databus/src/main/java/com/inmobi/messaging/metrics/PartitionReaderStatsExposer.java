package com.inmobi.messaging.metrics;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionReaderStatsExposer extends 
    DatabusConsumerStatsExposer {
  public final static String MESSAGES_READ_FROM_SOURCE = 
      "messagesReadFromSource";
  public final static String MESSAGES_ADDED_TO_BUFFER = "messagesAddedToBuffer";
  public final static String HANDLED_EXCEPTIONS = "handledExceptions";
  public final static String WAIT_TIME_UNITS_NEW_FILE = "waitTimeUnitsNewFile";
  public final static String PARTITION_CONTEXT = "PartitionId";
  public final static String CUMULATIVE_NANOS_FETCH_MESSAGE = 
      "cumulativeNanosForFecthMessage";
  public final static String NUMBER_RECORD_READERS = "numberRecordReaders";
  public final static String LIST = "list";
  public final static String OPEN = "open";
  public final static String GET_FILE_STATUS = "getFileStatus";
  public final static String EXISTS = "exists";

  private final AtomicLong numMessagesReadFromSource = new AtomicLong(0);
  private final AtomicLong numMessagesAddedToBuffer = new AtomicLong(0);
  private final AtomicLong numHandledExceptions = new AtomicLong(0);
  private final AtomicLong numWaitTimeUnitsNewFile = new AtomicLong(0);
  private final AtomicLong cumulativeNanosForFecthMessage = new AtomicLong(0);
  private final AtomicLong numberRecordReaders = new AtomicLong(0);
  private final AtomicLong listOps = new AtomicLong(0);
  private final AtomicLong openOps = new AtomicLong(0);
  private final AtomicLong fileStatusOps = new AtomicLong(0);
  private final AtomicLong existsOps = new AtomicLong(0);
  private final String pid;
  private final String fsUri;
  private final String FS_LIST, FS_OPEN, FS_GET_FILE_STATUS, FS_EXISTS;

  public PartitionReaderStatsExposer(String topicName, String consumerName,
      String pid, int consumerNumber, String fsUri) {
    super(topicName, consumerName, consumerNumber);
    this.pid = pid;
    this.fsUri = fsUri;
    FS_LIST = this.fsUri + "-" + LIST;
    FS_OPEN = this.fsUri + "-" + OPEN;
    FS_GET_FILE_STATUS = this.fsUri + "-" + GET_FILE_STATUS;
    FS_EXISTS = this.fsUri + "-" + EXISTS;
  }

  public void incrementMessagesReadFromSource() {
    numMessagesReadFromSource.incrementAndGet();
  }

  public void incrementMessagesAddedToBuffer() {
    numMessagesAddedToBuffer.incrementAndGet();
  }

  public void incrementHandledExceptions() {
    numHandledExceptions.incrementAndGet();
  }

  public void incrementWaitTimeUnitsNewFile() {
    numWaitTimeUnitsNewFile.incrementAndGet();
  }

  public void addCumulativeNanosFetchMessage(long nanos) {
    cumulativeNanosForFecthMessage.addAndGet(nanos);
  }

  public void incrementListOps(){
    listOps.incrementAndGet();
  }

  public void incrementOpenOps() {
    openOps.incrementAndGet();
  }

  public void incrementFileStatusOps() {
    fileStatusOps.incrementAndGet();
  }

  public void incrementExistsOps() {
    existsOps.incrementAndGet();
  }

  public void incrementNumberRecordReaders() {
    numberRecordReaders.incrementAndGet();
  }

  @Override
  protected void addToStatsMap(Map<String, Number> map) {
    map.put(MESSAGES_READ_FROM_SOURCE, getMessagesReadFromSource());
    map.put(MESSAGES_ADDED_TO_BUFFER, getMessagesAddedToBuffer());
    map.put(HANDLED_EXCEPTIONS, getHandledExceptions());
    map.put(WAIT_TIME_UNITS_NEW_FILE, getWaitTimeUnitsNewFile());
    map.put(CUMULATIVE_NANOS_FETCH_MESSAGE, getCumulativeNanosForFetchMessage());
    map.put(NUMBER_RECORD_READERS, getNumberRecordReaders());
    map.put(FS_LIST, getListOps());
    map.put(FS_OPEN, getOpenOps());
    map.put(FS_GET_FILE_STATUS, getFileStatusOps());
    map.put(FS_EXISTS, getExistsOps());
  }

  @Override
  protected void addToContextsMap(Map<String, String> map) {
    super.addToContextsMap(map);
    map.put(PARTITION_CONTEXT, pid);
  }

  public long getMessagesReadFromSource() {
    return numMessagesReadFromSource.get();
  }

  public long getMessagesAddedToBuffer() {
    return numMessagesAddedToBuffer.get();
  }

  public long getHandledExceptions() {
    return numHandledExceptions.get();
  }

  public long getWaitTimeUnitsNewFile() {
    return numWaitTimeUnitsNewFile.get();
  }

  public long getCumulativeNanosForFetchMessage() {
    return cumulativeNanosForFecthMessage.get();
  }

  public long getNumberRecordReaders() {
    return numberRecordReaders.get();
  }

  public long getListOps() {
    return listOps.get();
  }

  public long getOpenOps() {
    return openOps.get();
  }

  public long getFileStatusOps() {
    return fileStatusOps.get();
  }

  public long getExistsOps() {
    return existsOps.get();
  }
}
