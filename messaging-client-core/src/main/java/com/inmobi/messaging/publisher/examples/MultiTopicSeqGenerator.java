package com.inmobi.messaging.publisher.examples;

import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

public class MultiTopicSeqGenerator {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: MultiTopicSeqGenerator <topic1> <topic2> "
      		+ "<maxSeq>");
      return;
    }
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    String topic1 = args[0];
    String topic2 = args[1];
    long maxSeq = Integer.parseInt(args[2]);
    SeqGeneratorClient.publishMessages(publisher, topic1, maxSeq);
    SeqGeneratorClient.publishMessages(publisher, topic2, maxSeq);
    publisher.close();
    System.out.println("Total topic1 invocations: "
        + publisher.getStats(topic1).getInvocationCount());
    System.out.println("Total topic1 success: "
        + publisher.getStats(topic1).getSuccessCount());
    System.out.println("Total topic1 unhandledExceptions: "
        + publisher.getStats(topic1).getUnhandledExceptionCount());
    System.out.println("Total topic2 invocations: "
        + publisher.getStats(topic2).getInvocationCount());
    System.out.println("Total topic2 success: "
        + publisher.getStats(topic2).getSuccessCount());
    System.out.println("Total topic2 unhandledExceptions: "
        + publisher.getStats(topic2).getUnhandledExceptionCount());
  }
}
