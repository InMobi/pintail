package com.inmobi.messaging.consumer;

import com.inmobi.messaging.Message;

public interface MessageConsumer {

  Message next();

  void commit();

  void rollback();

  void close();
}
