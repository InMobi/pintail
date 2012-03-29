package com.inmobi.messaging;

public interface MessagePublisher {

  void init(String topic, ClientConfig config);
  void close();
  String getTopic();
  void publish(Message m);
}
