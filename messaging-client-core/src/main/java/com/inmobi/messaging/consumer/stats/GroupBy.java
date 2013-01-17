package com.inmobi.messaging.consumer.stats;

import java.util.HashSet;
import java.util.Set;

public class GroupBy {
  private static final String TIER = "tier";
  private static final String HOSTNAME = "hostname";
  private static final String TOPIC = "topic";

  public class Group {
    private String tier;
    private String hostname;
    private String topic;

    public Group(String tier, String hostname, String topic) {
      this.hostname = hostname;
      this.tier = tier;
      this.topic = topic;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      if (isSet(HOSTNAME)) {
      result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
      }
      if (isSet(TIER)) {
      result = prime * result + ((tier == null) ? 0 : tier.hashCode());
      }
      if (isSet(TOPIC)) {
      result = prime * result + ((topic == null) ? 0 : topic.hashCode());
      }
      return result;
    }

    @Override
    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append("[");
      if (isSet(TOPIC))
        buffer.append("topic = " + topic + ",");
      if (isSet(TIER))
        buffer.append("tier= " + tier + ",");
      if (isSet(HOSTNAME))
        buffer.append("hostname= " + hostname + ",");
      buffer.append("]");

      return buffer.toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Group other = (Group) obj;

      if (isSet(HOSTNAME)) {
        if (hostname == null) {
          if (other.hostname != null)
            return false;
        } else if (!hostname.equals(other.hostname))
          return false;
      }

      if (isSet(TIER)) {
        if (tier == null) {
          if (other.tier != null)
            return false;
        } else if (!tier.equals(other.tier))
          return false;
      }
      if (isSet(TOPIC)) {
        if (topic == null) {
          if (other.topic != null)
            return false;
        } else if (!topic.equals(other.topic))
          return false;
      }
      return true;
    }

  }

  private Set<String> isSet;

  public GroupBy(String input) {
    if (input == null)
      return;
    String[] columns = input.split(",");
    isSet = new HashSet<String>();
    for (String s : columns) {
      isSet.add(s.toLowerCase());
    }
  }

  private boolean isSet(String key) {
    return isSet.contains(key.toLowerCase());
  }

  public Group getGroup(String tier, String hostname, String topic) {
    return new Group(tier, hostname, topic);
  }

}
