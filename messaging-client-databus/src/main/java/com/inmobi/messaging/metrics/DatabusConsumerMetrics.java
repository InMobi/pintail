package com.inmobi.messaging.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.inmobi.messaging.consumer.MessageConsumerMetricsBase;

public class DatabusConsumerMetrics extends MessageConsumerMetricsBase {
  public DatabusConsumerMetrics(String topicName, String consumerName) {
    super(topicName, consumerName);
  }

  private List<PartitionReaderMetrics> preaderMetrics = 
      new ArrayList<PartitionReaderMetrics>();

  public void addPartitionReader(PartitionReaderMetrics metrics) {
    preaderMetrics.add(metrics);
  }

  @Override
  protected void addToStatsMap(Map<String, Number> map) {
    super.addToStatsMap(map);
    for (PartitionReaderMetrics metric : preaderMetrics) {
      metric.addToStatsMap(map);
    }
  }

  @Override
  protected void addToContextsMap(Map<String, String> map) {
    super.addToContextsMap(map);
    for (PartitionReaderMetrics metric : preaderMetrics) {
      metric.addToContextsMap(map);
    }
  }

}
