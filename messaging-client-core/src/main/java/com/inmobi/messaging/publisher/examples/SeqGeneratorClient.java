package com.inmobi.messaging.publisher.examples;

import java.nio.ByteBuffer;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

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
    publisher.close();
    long invocation = publisher.getStats(topic).getInvocationCount();
    System.out.println("Total invocations: " + invocation);
    System.out.println("Total success: " +
        publisher.getStats(topic).getSuccessCount());
    System.out.println("Total unhandledExceptions: " +
        publisher.getStats(topic).getUnhandledExceptionCount());
  }

  static void publishMessages(AbstractMessagePublisher publisher, String topic,
      long maxSeq) throws InterruptedException {
    for (long seq = 1; seq <= maxSeq; seq++) {
      Message msg = new Message(ByteBuffer.wrap(Long.toString(seq).getBytes()));
      publisher.publish(topic, msg);
      Thread.sleep(1);
    }    
  }
}
