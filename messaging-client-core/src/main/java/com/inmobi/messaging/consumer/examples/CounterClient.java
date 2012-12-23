package com.inmobi.messaging.consumer.examples;

import java.util.Calendar;

import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;

/**
 * Creates DatabusConsumer from  configuration in classpath and
 * counts the messages. 
 * 
 * Starts consuming messages from specified time in the argument. Prints the
 * counter after every 1000 messages.
 * Does marking after every 5000 messages.
 */
public class CounterClient {

  public static void main(String[] args) throws Exception {
    MessageConsumer consumer;
    if (args.length == 0) {
      System.out.println("start time is not provided. Starts from the last marked position");
      consumer = MessageConsumerFactory.create();
    } else if (args.length == 1) {
      Calendar now = Calendar.getInstance();
      Integer min = Integer.parseInt(args[0]);
      now.add(Calendar.MINUTE, - (min.intValue()));
      consumer = MessageConsumerFactory.create(now.getTime());
    } else {
      System.out.println("Usage: counterclient <minutes-to-read-from> ");
      System.exit(-1);
    }
    
    int msgCounter = 0;
    int markCounter = 0;
    while (true) {
      for (int i = 0; i < 1000; i++) {
        consumer.next();
        msgCounter++;
      }
      System.out.println("Counter:" + msgCounter);
      markCounter++;
      if (markCounter == 5) {
        consumer.mark();
        markCounter = 0;
      }
    }
  }
}
