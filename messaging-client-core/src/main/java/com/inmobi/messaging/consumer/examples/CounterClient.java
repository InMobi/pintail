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

  public static void close() throws IOException {
    if (consumer != null) {
      consumer.mark();
      consumer.close();
      System.out.println("Counter value: " + msgCounter);
      consumer =  null;
    }
  }

  public static void main(String[] args) throws Exception {
    final Thread mainThread = Thread.currentThread();
    long timeout = 300;
    if (args.length == 0) {
      System.out.println("start time is not provided. Starts from the last " +
          "marked position");
      consumer = MessageConsumerFactory.create();
    } else if (args.length >= 1) {
      Calendar now = Calendar.getInstance();
      Integer min = Integer.parseInt(args[0]);

      if (args.length == 2) {
        timeout = Long.parseLong(args[1]);
      }
      now.add(Calendar.MINUTE, - (min.intValue()));
      consumer = MessageConsumerFactory.create(now.getTime());
    } else {
      consumer = null;
      System.out.println("Usage: counterclient [<minutes-to-read-from>] " +
          "[<time-to-wait-NextMessage>]");
      System.exit(-1);
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        keepRunnig = false;
        try {
          mainThread.interrupt();
          close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });

    while (keepRunnig) {
      try {
        for (int i = 0; i < 1000; i++) {
          Message msg = consumer.next(timeout, TimeUnit.SECONDS);
          if (msg == null) {
            keepRunnig = false;
            break;
          }
          msgCounter++;
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
    close();
  } 
}
