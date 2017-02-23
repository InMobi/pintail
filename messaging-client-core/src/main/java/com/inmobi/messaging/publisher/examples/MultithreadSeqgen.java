package com.inmobi.messaging.publisher.examples;

/*
 * #%L
 * messaging-client-core
 * %%
 * Copyright (C) 2012 - 2014 InMobi
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

import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

/**
 * Sends messages from multiple threads.
 *
 */
public class MultithreadSeqgen {

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: MultithreadSeqGeneratorClient <topic>"
          + " <maxSeq> <number_of_threads>");
      return;
    }
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    String topic = args[0];
    long maxSeq = Integer.parseInt(args[1]);
    int totalThread = Integer.parseInt(args[2]);
    PublishThreadNew[] p = new PublishThreadNew[totalThread];
    long threadMaxSeq = maxSeq / totalThread;
    for (int thread = 0; thread < totalThread; thread++) {
      p[thread] = new PublishThreadNew(topic, publisher, threadMaxSeq);
      p[thread].start();
    }
    for (int thread = 0; thread < totalThread; thread++) {
      p[thread].join();
    }

    SeqGeneratorClient.waitToComplete(publisher, topic);
    publisher.close();
    long invocation = publisher.getStats(topic).getInvocationCount();
    System.out.println("Total invocations: " + invocation);
    System.out.println("Total success: "
        + publisher.getStats(topic).getSuccessCount());
    System.out.println("Total unhandledExceptions: "
        + publisher.getStats(topic).getUnhandledExceptionCount());
  }

  private static class PublishThreadNew extends Thread {
    private String topic;
    private AbstractMessagePublisher publisher;
    private long maxSeq;

    PublishThreadNew(String topic,
        AbstractMessagePublisher publisher, long maxSeq) {
      this.topic = topic;
      this.publisher = publisher;
      this.maxSeq = maxSeq;
    }

    public void run() {
      try {
        SeqGeneratorClient.publishMessages(publisher, topic, maxSeq);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
