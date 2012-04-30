package com.inmobi.messaging.publisher.examples;

import java.util.Map;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;

public class ConsolePublisher extends AbstractMessagePublisher {

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    System.out.println(m.getData().asCharBuffer().toString());
  }

}
