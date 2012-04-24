package com.inmobi.messaging.example;

import java.nio.ByteBuffer;

import com.inmobi.messaging.AbstractMessagePublisher;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.MessagePublisherFactory;

public class SeqGeneratorClient {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: SeqGeneratorClient <maxSeq>");
      return;
    }
    AbstractMessagePublisher publisher = (AbstractMessagePublisher) MessagePublisherFactory
        .create();
    long maxSeq = Integer.parseInt(args[0]);
    for (long seq = 1; seq <= maxSeq; seq++) {
      Message msg = new Message("testclient", 
          ByteBuffer.wrap(Long.toString(seq).getBytes()));
      publisher.publish(msg);
      if (seq % 10000 == 0) {
        Thread.sleep(2000);
      }
    }
    waitToComplete(publisher);
    Thread.sleep(5000);
    //publisher.close();
    long invocation = publisher.getStats().getInvocationCount();
    System.out.println("Total invocations: " + invocation);
    System.out.println("Total success: " + publisher.getStats().getSuccessCount());
    System.out.println("Total unhandledExceptions: " + publisher.getStats().getUnhandledExceptionCount());
  }

  private static void waitToComplete(AbstractMessagePublisher publisher)
      throws InterruptedException {
    int i = 0;
    while (publisher.getStats().getInFlight() != 0 && i++ < 10) {
      System.out.println("Inflight: "+ publisher.getStats().getInFlight());
      Thread.sleep(100);
    }
  }
}
