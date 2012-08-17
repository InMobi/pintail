package com.inmobi.messaging.consumer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;

/**
 * Base metrics class for MessageConsumer.
 */
public class BaseMessageConsumerStatsExposer extends 
    AbstractMessagingClientStatsExposer implements 
    MessageConsumerMetricsConstants {
  private final AtomicLong numMessagesConsumed = new AtomicLong(0);
  private final AtomicLong numMarkCalls = new AtomicLong(0);
  private final AtomicLong numResetCalls = new AtomicLong(0);
  private final String topicName;
  private final String consumerName;

  public BaseMessageConsumerStatsExposer(String topicName, String consumerName)
  {
    this.topicName = topicName;
    this.consumerName = consumerName;
  }
  public void addMessagesConsumed() {
    numMessagesConsumed.incrementAndGet();
  }

  public void addMarkCalls() {
    numMarkCalls.incrementAndGet();
  }

  public void addResetCalls() {
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

  protected void addToContextsMap(Map<String, String> contexts) {
    contexts.put(TOPIC_CONTEXT, topicName);
    contexts.put(CONSUMER_CONTEXT, consumerName);    
  }
}
