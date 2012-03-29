package com.inmobi.messaging;

import java.util.Arrays;

public final class Message {

  private final byte message[];

  public Message(byte message[]) {
    this.message = message;
  }

  public byte[] getMessage() {
    return message;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(message);
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
    if (!Arrays.equals(message, other.message))
      return false;
    return true;
  }
}
