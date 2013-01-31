package com.inmobi.messaging.publisher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.inmobi.messaging.Message;

public class MockInMemoryPublisher extends AbstractMessagePublisher {

  public static Map<String, BlockingQueue<Message>> source = new HashMap<String, BlockingQueue<Message>>();

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    // TODO Auto-generated method stub
    String topic = headers.get(AbstractMessagePublisher.HEADER_TOPIC);
    if (source.get(topic) == null) {
      BlockingQueue<Message> list = new LinkedBlockingQueue<Message>();
      list.add(m);
      source.put(topic, list);
    } else {
      source.get(topic).add(m);
    }

  }

}
