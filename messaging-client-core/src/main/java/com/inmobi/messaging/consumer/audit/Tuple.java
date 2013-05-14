package com.inmobi.messaging.consumer.audit;

import java.util.Date;
import java.util.Map;

public class Tuple {
  private String name;
  private String tier;
  private String cluster;
  private Date timestamp;
  private Map<LatencyColumns, Long> latencyCountMap;

  public Tuple(String name, String tier, String cluster, Date timestamp,
               Map<LatencyColumns, Long> latencyCountMap) {
    this.name = name;
    this.tier = tier;
    this.cluster = cluster;
    this.timestamp = timestamp;
    this.latencyCountMap = latencyCountMap;
  }

  public String getName() {
    return name;
  }

  public String getTier() {
    return tier;
  }

  public String getCluster() {
    return cluster;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public Map<LatencyColumns, Long> getLatencyCountMap() {
    return latencyCountMap;
  }

  public void setLatencyCountMap(Map<LatencyColumns, Long> latencyCountMap) {
    this.latencyCountMap = latencyCountMap;
  }

  @Override
  public String toString() {
    return "Tuple{" +
        "name='" + name + '\'' +
        ", tier='" + tier + '\'' +
        ", cluster='" + cluster + '\'' +
        ", timestamp=" + timestamp +
        ", latencyCountMap=" + latencyCountMap +
        '}';
  }
}
