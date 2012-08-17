package com.inmobi.messaging.consumer;

import java.util.Map;

import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;

public abstract class AbstractMessageConsumerStatsExposer extends 
    AbstractMessagingClientStatsExposer implements 
    MessageConsumerMetricsConstants {

  private final String topicName;
  private final String consumerName;

  protected AbstractMessageConsumerStatsExposer(String topicName,
      String consumerName) {
    this.topicName = topicName;
    this.consumerName = consumerName;
  }

  protected void addToContextsMap(Map<String, String> contexts) {
    contexts.put(TOPIC_CONTEXT, topicName);
    contexts.put(CONSUMER_CONTEXT, consumerName);    
  }
}
