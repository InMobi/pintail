package com.inmobi.messaging.consumer.stats;

import java.util.Date;

import com.inmobi.audit.thrift.AuditPacket;

public class Filter {

  private String tier, hostname, topic;
  private Date fromTime, toTime;

  public Filter(String tier, String hostname, String topic, Date fromTime,
      Date toTime) {
    this.tier = tier;
    this.hostname = hostname;
    this.topic = topic;
    this.fromTime = fromTime;
    this.toTime = toTime;
  }

  public boolean apply(AuditPacket packet) {

    if (tier != null && !packet.getTier().equalsIgnoreCase(tier))
      return false;
    if (hostname != null && !packet.getHostname().equalsIgnoreCase(hostname))
      return false;
    if (topic != null && !packet.getTopic().equalsIgnoreCase(topic))
      return false;
    return isWithinRange(packet.getTimestamp());
  }

  private boolean isWithinRange(long timestamp) {
    return (timestamp >= fromTime.getTime() && timestamp < toTime.getTime());
  }
}
