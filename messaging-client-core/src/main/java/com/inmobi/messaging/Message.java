package com.inmobi.messaging;

import java.nio.ByteBuffer;

public final class Message {

  private final String topic;
  private final ByteBuffer data;

  public Message(String topic, ByteBuffer data) {
    this.topic = topic;
    this.data = data;
  }

  public Message(String topic, byte[] data) {
    this.topic = topic;
    this.data = ByteBuffer.wrap(data);
  }

  public String getTopic() {
    return topic;
  }

  public ByteBuffer getData() {
    return data;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((data == null) ? 0 : data.hashCode());
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
    Message other = (Message) obj;
    if (data == null) {
      if (other.data != null)
        return false;
    } else if (!data.equals(other.data))
      return false;
    if (topic == null) {
      if (other.topic != null)
        return false;
    } else if (!topic.equals(other.topic))
      return false;
    return true;
  }
}
