package com.inmobi.messaging.consumer.examples;

import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import com.inmobi.messaging.Message;
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
  static MessageConsumer consumer;
  static int msgCounter;
  static int markCounter;
  static volatile boolean keepRunnig = true;
  public static void main(String[] args) throws Exception {
    final Thread mainThread = Thread.currentThread();
    if (args.length == 0) {
      System.out.println("start time is not provided. Starts from the last " +
          "marked position");
      consumer = MessageConsumerFactory.create();
    } else if (args.length == 1) {
      Calendar now = Calendar.getInstance();
      Integer min = Integer.parseInt(args[0]);
      now.add(Calendar.MINUTE, - (min.intValue()));
      consumer = MessageConsumerFactory.create(now.getTime());
    } else {
      consumer = null;
      System.out.println("Usage: counterclient <minutes-to-read-from> ");
      System.exit(-1);
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        keepRunnig = false;
        try {
          if (consumer != null) {
            mainThread.interrupt();
            consumer.mark();
            consumer.close();
            System.out.println("Counter value: " + msgCounter);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    while (keepRunnig) {
      try {
        for (int i = 0; i < 1000; ) {
          Message msg = consumer.next(1000, TimeUnit.MILLISECONDS);
          if (msg != null) {
            msgCounter++;
            i++;
          }
        }
        System.out.println("Counter:" + msgCounter);
        markCounter++;
        if (markCounter == 5) {
          consumer.mark();
          markCounter = 0;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
