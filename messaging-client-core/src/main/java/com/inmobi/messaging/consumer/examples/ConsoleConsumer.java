package com.inmobi.messaging.consumer.examples;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;

public class ConsoleConsumer {

  public static void main(String[] args) throws Exception {
    MessageConsumer consumer = MessageConsumerFactory.create();
    
    while (true) {
      Message msg = consumer.next();
      System.out.println("MESSAGE:" + new String(msg.getData().array()));
    }
  }
}
