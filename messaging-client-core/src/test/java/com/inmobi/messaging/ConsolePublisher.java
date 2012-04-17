package com.inmobi.messaging;

import java.util.Map;

public class ConsolePublisher extends AbstractMessagePublisher {

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    System.out.println(new String(m.getMessage()));
  }

}
