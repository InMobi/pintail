package com.inmobi.messaging;

public interface MessagePublisher {

  void init(ClientConfig config);
  void close();
  void publish(Message m);
}
