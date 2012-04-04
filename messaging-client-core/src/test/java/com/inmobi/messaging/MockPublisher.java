package com.inmobi.messaging;

import java.util.Map;

public class MockPublisher extends AbstractMessagePublisher {
  public static Message msg;
  public static void reset() {
    msg = null;
  }
  @Override
  protected void publish(Map<String, String> headers, Message m) {
    msg = m;
  }
}
