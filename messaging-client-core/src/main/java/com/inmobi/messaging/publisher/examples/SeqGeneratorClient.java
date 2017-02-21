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

import java.nio.ByteBuffer;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.PintailException;

/**
 * Publishes integer sequence upto <code>maxSeq</code> on the <code>topic</code>,
 * each integer as message.
 *
 * Prints out the publisher statistics at the end of the publishing.
 */
public class SeqGeneratorClient {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: SeqGeneratorClient <topic> <maxSeq>");
      return;
    }
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    String topic = args[0];
    long maxSeq = Integer.parseInt(args[1]);
    publishMessages(publisher, topic, maxSeq);
    waitToComplete(publisher, topic);
    publisher.close();
    long invocation = publisher.getStats(topic).getInvocationCount();
    System.out.println("Total invocations: " + invocation);
    System.out.println("Total success: "
        + publisher.getStats(topic).getSuccessCount());
    System.out.println("Total unhandledExceptions: "
        + publisher.getStats(topic).getUnhandledExceptionCount());
  }

  static void publishMessages(AbstractMessagePublisher publisher, String topic,
      long maxSeq) throws InterruptedException {
    for (long seq = 1; seq <= maxSeq; seq++) {
      Message msg = new Message(ByteBuffer.wrap(Long.toString(seq).getBytes()));
      try {
        publisher.publish(topic, msg);
      } catch (PintailException e) {
        e.printStackTrace(); //utility method
      }
      Thread.sleep(1);
    }
  }

  static void waitToComplete(AbstractMessagePublisher publisher,
      String topic) throws InterruptedException {
    while (publisher.getStats(topic).getInFlight() > 0) {
      System.out.println("Inflight: " + publisher.getStats(topic).getInFlight());
      Thread.sleep(100);
    }
  }
}
