package com.inmobi.messaging.publisher.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

/**
 * Publishes each line of of the <code>file</code> as a message on the 
 * <code>topic</code>.
 * 
 * Prints out the publisher statistics at the end of the publishing.
 */
public class FileLoggerClient {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: FileLogger <topic> <file>");
      return;
    }
    String topic = args[0];
    String file = args[1];
    AbstractMessagePublisher publisher = 
        (AbstractMessagePublisher) MessagePublisherFactory
        .create();
    BufferedReader in = new BufferedReader(new FileReader(new File(file)));
    String line = in.readLine();
    while (line != null) {
      Message msg = new Message(ByteBuffer.wrap(line.getBytes()));
      publisher.publish(topic, msg);
      Thread.sleep(1);
      line = in.readLine();
    }
    waitToComplete(publisher);
    Thread.sleep(5000);
    publisher.close();
    long invocation = publisher.getStats().getInvocationCount();
    System.out.println("Total invocations: " + invocation);
    System.out.println("Total success: " + publisher.getStats().getSuccessCount());
    System.out.println("Total unhandledExceptions: " +
      publisher.getStats().getUnhandledExceptionCount());
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
