package com.inmobi.messaging.consumer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base metrics class for MessageConsumer.
 */
public class BaseMessageConsumerStatsExposer extends 
    AbstractMessageConsumerStatsExposer {
  private final AtomicLong numMessagesConsumed = new AtomicLong(0);
  private final AtomicLong numMarkCalls = new AtomicLong(0);
  private final AtomicLong numResetCalls = new AtomicLong(0);

  public BaseMessageConsumerStatsExposer(String topicName, String consumerName)
  {
    super(topicName, consumerName);
  }

  public void incrementMessagesConsumed() {
    numMessagesConsumed.incrementAndGet();
  }

  public void incrementMarkCalls() {
    numMarkCalls.incrementAndGet();
  }

  public void incrementResetCalls() {
    numResetCalls.incrementAndGet();
  }

  public long getNumMessagesConsumed() {
    return numMessagesConsumed.get();
  }
  
  public long getNumMarkCalls() {
    return numMarkCalls.get();
  }

  public long getNumResetCalls() {
    return numResetCalls.get();
  }

  @Override
  protected void addToStatsMap(Map<String, Number> statsMap) {
    statsMap.put(MESSAGES_CONSUMED, getNumMessagesConsumed());
    statsMap.put(MARK_CALLS, getNumMarkCalls());
    statsMap.put(RESET_CALLS, getNumResetCalls());
  }
}
