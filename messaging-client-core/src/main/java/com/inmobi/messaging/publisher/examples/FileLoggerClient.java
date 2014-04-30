package com.inmobi.messaging.publisher.examples;

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
    publisher.close();
    long invocation = publisher.getStats(topic).getInvocationCount();
    System.out.println("Total invocations: " + invocation);
    System.out.println("Total success: " + publisher.getStats(topic)
        .getSuccessCount());
    System.out.println("Total unhandledExceptions: "
        + publisher.getStats(topic).getUnhandledExceptionCount());
  }
}
