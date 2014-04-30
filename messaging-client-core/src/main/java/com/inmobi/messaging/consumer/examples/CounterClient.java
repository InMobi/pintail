package com.inmobi.messaging.consumer.examples;

/*
 * #%L
 * messaging-client-core
 * %%
 * Copyright (C) 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.EndOfStreamException;
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
  static boolean closed = false;

  public static synchronized void close() throws IOException {
    if (!closed) {
      if (consumer != null) {
        consumer.mark();
        consumer.close();
        System.out.println("Counter value: " + msgCounter);
      }
      closed = true;
    }
  }

  public static void main(String[] args) throws Exception {
    final Thread mainThread = Thread.currentThread();
    long timeout = -1;
    Integer min = -1;
    long numOfMsgsToBeConsumed = -1;
    if (args.length <= 3) {
      if (args.length >= 1) {
        min = Integer.parseInt(args[0]);
      }
      if (args.length >= 2) {
        timeout = Long.parseLong(args[1]);
      }
      if (args.length >= 3) {
        numOfMsgsToBeConsumed = Long.parseLong(args[2]);
      }
    } else {
      consumer = null;
      System.out.println("Usage: counter [<minutes-to-read-from> "
          + " <time-to-wait-NextMessage> <maxNumMessages>]");
      System.exit(-1);
    }

    if (timeout == -1) {
      // set default value if timeout is not provided or -1 provided
      timeout = 300;
    }

    if (min != -1) {
      Calendar now = Calendar.getInstance();
      now.add(Calendar.MINUTE, -(min.intValue()));
      consumer = MessageConsumerFactory.create(now.getTime());
    } else {
      consumer = MessageConsumerFactory.create();
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
          try {
            Message msg = consumer.next(timeout, TimeUnit.SECONDS);
            if (msg == null) {
              keepRunnig = false;
              break;
            }
            msgCounter++;
            if (msgCounter == numOfMsgsToBeConsumed) {
              keepRunnig = false;
              break;
            }
          } catch (EndOfStreamException e) {
            keepRunnig = false;
            break;
          }
        }
        if (keepRunnig) {
          System.out.println("Counter:" + msgCounter);
          markCounter++;
          if (markCounter == 5) {
            consumer.mark();
            markCounter = 0;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    close();
  }
}
