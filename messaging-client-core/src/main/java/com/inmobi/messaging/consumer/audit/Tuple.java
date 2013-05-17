package com.inmobi.messaging.consumer.audit;

import java.util.Date;
import java.util.Map;

public class Tuple {
  final private String hostname;
  final private String tier;
  final private String cluster;
  final private Date timestamp;
  final private String topic;
  private long sent, received;

  private Map<LatencyColumns, Long> latencyCountMap;

  public Tuple(String hostname, String tier, String cluster, Date timestamp,
      String topic) {
    this.hostname = hostname;
    this.tier = tier;
    this.cluster = cluster;
    this.timestamp = timestamp;
    this.topic = topic;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
    result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
    result = prime * result + ((tier == null) ? 0 : tier.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    result = prime * result + ((topic == null) ? 0 : topic.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Tuple other = (Tuple) obj;
    if (cluster == null) {
      if (other.cluster != null)
        return false;
    } else if (!cluster.equals(other.cluster))
      return false;
    if (hostname == null) {
      if (other.hostname != null)
        return false;
    } else if (!hostname.equals(other.hostname))
      return false;
    if (tier == null) {
      if (other.tier != null)
        return false;
    } else if (!tier.equals(other.tier))
      return false;
    if (timestamp == null) {
      if (other.timestamp != null)
        return false;
    } else if (!timestamp.equals(other.timestamp))
      return false;
    if (topic == null) {
      if (other.topic != null)
        return false;
    } else if (!topic.equals(other.topic))
      return false;
    return true;
  }

  public String getTier() {
    return tier;
  }

  public long getSent() {
    return sent;
  }

  public void setSent(long sent) {
    this.sent = sent;
  }

  public long getReceived() {
    return received;
  }

  public void setReceived(long received) {
    this.received = received;
  }

  public String getCluster() {
    return cluster;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public String getHostname() {
    return hostname;
  }

  public String getTopic() {
    return topic;
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
        "name='" + hostname + '\'' +
        ", tier='" + tier + '\'' +
        ", cluster='" + cluster + '\'' +
        ", timestamp=" + timestamp +
        ", latencyCountMap=" + latencyCountMap +
        '}';
  }
}
