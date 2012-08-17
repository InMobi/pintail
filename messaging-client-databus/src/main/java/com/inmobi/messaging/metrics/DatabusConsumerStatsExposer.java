package com.inmobi.messaging.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.inmobi.messaging.consumer.BaseMessageConsumerStatsExposer;

public class DatabusConsumerStatsExposer extends 
    BaseMessageConsumerStatsExposer {
  public DatabusConsumerStatsExposer(String topicName, String consumerName) {
    super(topicName, consumerName);
  }

  private List<PartitionReaderStatsExposer> preaderMetrics = 
      new ArrayList<PartitionReaderStatsExposer>();

  public void addPartitionReader(PartitionReaderStatsExposer metrics) {
    preaderMetrics.add(metrics);
  }

  @Override
  protected void addToStatsMap(Map<String, Number> map) {
    super.addToStatsMap(map);
    for (PartitionReaderStatsExposer metric : preaderMetrics) {
      metric.addToStatsMap(map);
    }
  }

  @Override
  protected void addToContextsMap(Map<String, String> map) {
    super.addToContextsMap(map);
    for (PartitionReaderStatsExposer metric : preaderMetrics) {
      metric.addToContextsMap(map);
    }
  }

}
