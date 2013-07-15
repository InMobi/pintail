package com.inmobi.messaging.consumer.examples;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.EndOfStreamException;
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
      try {
        Message msg = consumer.next();
        System.out.println("MESSAGE:" + new String(msg.getData().array()));
      } catch (EndOfStreamException e) {
        System.exit(0);
      }
    }
  }
}
