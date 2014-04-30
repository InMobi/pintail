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
