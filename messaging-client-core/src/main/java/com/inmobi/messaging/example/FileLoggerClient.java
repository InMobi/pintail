package com.inmobi.messaging.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;

import com.inmobi.messaging.AbstractMessagePublisher;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.MessagePublisherFactory;

public class FileLoggerClient {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: FileLogger <file>");
      return;
    }
    String file = args[0];
    AbstractMessagePublisher publisher = 
        (AbstractMessagePublisher) MessagePublisherFactory
        .create();
    BufferedReader in = new BufferedReader(new FileReader(new File(file)));
    String line = in.readLine();
    while (line != null) {
      Message msg = new Message("test", ByteBuffer.wrap(line.getBytes()));
      publisher.publish(msg);
      line = in.readLine();
    }
    publisher.close();
    long invocation = publisher.getStats().getInvocationCount();
    System.out.println("Total invocations: " + invocation);
  }
}
