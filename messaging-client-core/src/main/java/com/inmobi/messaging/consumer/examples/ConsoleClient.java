package com.inmobi.messaging.consumer.examples;

import java.util.concurrent.TimeUnit;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;

/**
 * Creates DatabusConsumer from  configuration in classpath and consumes
 * messages to print on the screen. 
 */
public class ConsoleClient {

  public static void main(String[] args) throws Exception {
    MessageConsumer consumer = MessageConsumerFactory.create();
    
    while (true) {
      Message msg = consumer.next(1000, TimeUnit.MILLISECONDS);
      if (msg != null) {
        System.out.println("MESSAGE:" + new String(msg.getData().array()));
      }
    }
  }
}
