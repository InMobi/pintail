package com.inmobi.messaging.consumer.audit;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.inmobi.audit.thrift.AuditMessage;

/**
 * This class is responsible for reading audit packets,aggregating stats in
 * memory for some time and than performing batch update of the DB
 * 
 * @author rohit.kochar
 * 
 */
public class AuditStatsFeeder {

  private class TupleKey {
    public TupleKey(Date timestamp, String tier, String topic, String hostname,
        String cluster) {
      this.timestamp = timestamp;
      this.tier = tier;
      this.topic = topic;
      this.hostname = hostname;
      this.cluster = cluster;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
      result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
      result = prime * result + ((tier == null) ? 0 : tier.hashCode());
      result = prime * result
          + ((timestamp == null) ? 0 : timestamp.hashCode());
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
      TupleKey other = (TupleKey) obj;
      if (!getOuterType().equals(other.getOuterType()))
        return false;
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

    Date timestamp;
    String tier, topic, hostname, cluster;

    private AuditStatsFeeder getOuterType() {
      return AuditStatsFeeder.this;
    }
  }

  private Map<TupleKey, Tuple> tuples = new HashMap<TupleKey, Tuple>();

  private void addTuples(AuditMessage message,String cluster) {
    if(message==null)
      return;
    int windowSize = message.getWindowSize();
    for (long timestamp : message.getReceived().keySet()) {
      long upperBoundaryTime = timestamp + windowSize * 1000;
      TupleKey key = new TupleKey(new Date(upperBoundaryTime),
          message.getTier(), message.getTopic(), message.getHostname(), cluster);
      if (tuples.containsKey(key)) {
        Tuple tuple = tuples.get(key);
        long received = message.getReceived().get(timestamp);
        long sent = message.getSent().get(timestamp);
        tuple.setSent(tuple.getSent() + sent);
        tuple.setReceived(tuple.getReceived() + received);
      }
    }

  }
}
