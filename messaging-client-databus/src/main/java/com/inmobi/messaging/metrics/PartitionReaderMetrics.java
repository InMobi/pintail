package com.inmobi.messaging.metrics;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.inmobi.instrumentation.MessagingClientMetrics;

public class PartitionReaderMetrics extends MessagingClientMetrics {
  private final static String MESSAGES_READ_FROM_SOURCE = 
      "messagesReadFromSource";
  private final static String MESSAGES_ADDED_TO_BUFFER = 
      "messagesAddedToBuffer";
  private final static String HANDLED_EXCEPTIONS = "handledExceptions";
  private final static String WAIT_TIME_UNITS_NEW_DATA = "waitTimeUnitsNewData";
  private final static String CONTEXT_SEPERATOR = "-";

  private final AtomicLong numMessagesReadFromSource = new AtomicLong(0);
  private final AtomicLong numMessagesAddedToBuffer = new AtomicLong(0);
  private final AtomicLong numHandledExceptions = new AtomicLong(0);
  private final AtomicLong numWaitTimeUnitsNewData = new AtomicLong(0);

  private final String pid;
  protected final String pidContextStr;

  public PartitionReaderMetrics(String pid) {
    this.pid = pid;
    this.pidContextStr = this.pid + CONTEXT_SEPERATOR;
  }

  public void addMessagesReadFromSource() {
    numMessagesReadFromSource.incrementAndGet();
  }

  public void addMessagesAddedToBuffer() {
    numMessagesAddedToBuffer.incrementAndGet();
  }

  public void addHandledExceptions() {
    numHandledExceptions.incrementAndGet();
  }

  public void addWaitTimeUnitsNewData() {
    numWaitTimeUnitsNewData.incrementAndGet();
  }

  @Override
  protected void addToStatsMap(Map<String, Number> map) {
    map.put(pidContextStr + MESSAGES_READ_FROM_SOURCE,
        getMessagesReadFromSource());
    map.put(pidContextStr + MESSAGES_ADDED_TO_BUFFER,
        getMessagesAddedToBuffer());
    map.put(pidContextStr + HANDLED_EXCEPTIONS,
        getHandledExceptions());
    map.put(pidContextStr + WAIT_TIME_UNITS_NEW_DATA,
        getWaitTimeUnitsNewData());
  }

  @Override
  protected void addToContextsMap(Map<String, String> map) {    
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

  public long getWaitTimeUnitsNewData() {
    return numWaitTimeUnitsNewData.get();
  }
}
