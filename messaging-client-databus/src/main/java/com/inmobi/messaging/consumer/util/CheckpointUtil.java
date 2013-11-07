package com.inmobi.messaging.consumer.util;

import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;

public class CheckpointUtil {

  public static void main(String [] args) throws Exception {
    String confFile = null;
    if (args.length > 0) {
      confFile = args[0];
    }
    System.setProperty("consumer.checkpoint.migrate", "true");
    MessageConsumer consumer;
    if (confFile != null) {
      consumer = MessageConsumerFactory.create(confFile);
    } else {
      consumer = MessageConsumerFactory.create();
    }
    consumer.mark();
    System.setProperty("consumer.checkpoint.migrate", "false");
    consumer.close();
  }
}


