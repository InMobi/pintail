package com.inmobi.messaging.example;

import java.util.Map;

import com.inmobi.messaging.AbstractMessagePublisher;
import com.inmobi.messaging.Message;

public class ConsolePublisher extends AbstractMessagePublisher {

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    System.out.println(m.getData().asCharBuffer().toString());
  }

}
