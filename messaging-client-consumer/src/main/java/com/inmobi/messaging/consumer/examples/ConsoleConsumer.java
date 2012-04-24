package com.inmobi.messaging.consumer.examples;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;

public class ConsoleConsumer {

  public static void main(String[] args) throws Exception {
    ClientConfig config = new ClientConfig();
    MessageConsumer consumer = MessageConsumerFactory.create(config);
    
    while (true) {
      Message msg = consumer.next();
      System.out.println("MESSAGE:" + new String(msg.getData().array()));
    }
  }
}
