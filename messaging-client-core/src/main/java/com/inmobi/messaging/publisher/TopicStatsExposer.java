package com.inmobi.messaging.publisher;

import java.util.Map;

import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;
import com.inmobi.instrumentation.TimingAccumulator;

public class TopicStatsExposer extends AbstractMessagingClientStatsExposer {

  public static final String STATS_TYPE_CONTEXT_NAME = "messaging_type";
  public static final String TOPIC_CONTEXT_NAME = "category";
  public static final String STATS_TYPE = "application";

  final String topic;
  final TimingAccumulator stats;
  public TopicStatsExposer(String topicName,
      TimingAccumulator timingAccumulator) {
    this.stats = timingAccumulator;
    this.topic = topicName;
  }

  public TimingAccumulator getTimingAccumulator() {
    return stats;
  }

  @Override
  protected void addToStatsMap(Map<String, Number> map) {
    map.putAll(stats.getMap());
  }

  @Override
  protected void addToContextsMap(Map<String, String> map) {
    map.put(STATS_TYPE_CONTEXT_NAME, STATS_TYPE);
    map.put(TOPIC_CONTEXT_NAME, topic);
  }
}
