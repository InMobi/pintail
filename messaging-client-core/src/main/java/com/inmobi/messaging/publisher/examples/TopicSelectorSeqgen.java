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

import com.inmobi.messaging.ClientConfig;
import java.nio.ByteBuffer;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.util.TopicSelector;

public class TopicSelectorSeqgen {

  public static void main(String[] args) throws Exception {
    ClientConfig conf = new ClientConfig();
    if (args.length != 2) {
      System.err.println("Usage: TopicSelectorExample"
          + " <topic> <maxSeq>");
      return;
    }
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    String top = args[0];
    long maxSeq = Integer.parseInt(args[1]);

    TopicSelector.setSelectorClass(conf, top,
        MsgValueTopicSelector.class.getName());
    MsgValueTopicSelector selector = (MsgValueTopicSelector)
        TopicSelector.create(top, conf);

    for (long seq = 1; seq <= maxSeq; seq++) {
      String str1 = Long.toString(seq);
      TopicMessage msg1 = new TopicMessage(1, str1);
      TopicMessage msg2 = new TopicMessage(2, str1);

      Message msg = new Message(ByteBuffer.wrap((msg1.toString().getBytes())));
      publisher.publish(selector.selectTopic(msg1), msg);

      Message mssg = new Message(ByteBuffer.wrap((msg2.toString().getBytes())));
      publisher.publish(selector.selectTopic(msg2), mssg);


      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    String top1 = selector.selectTopic(new TopicMessage(1, ""));
    String top2 = selector.selectTopic(new TopicMessage(2, ""));

    publisher.close();
    selector.close();

    System.out.println("Total topic invocations: "
        + publisher.getStats(top1).getInvocationCount());
    System.out.println("Total topic success: "
        + publisher.getStats(top1).getSuccessCount());
    System.out.println("Total topic unhandledExceptions: "
        + publisher.getStats(top1).getUnhandledExceptionCount());

    System.out.println("Total topic invocations: "
        + publisher.getStats(top2).getInvocationCount());
    System.out.println("Total topic success: "
        + publisher.getStats(top2).getSuccessCount());
    System.out.println("Total topic unhandledExceptions: "
        + publisher.getStats(top2).getUnhandledExceptionCount());
  }

  public static class MsgValueTopicSelector extends TopicSelector<TopicMessage> {
    private String logicalTopic;

    @Override
    protected void init(String logicalTopic, ClientConfig conf) {
      this.logicalTopic = logicalTopic;
    }

    @Override
    public String selectTopic(TopicMessage object) {
      return logicalTopic + object.getIndex();
    }

    public String getLogicalTopic() {
      return logicalTopic;
    }
  }
}

class TopicMessage {
  int index;
  String message;
  TopicMessage(int index, String message) {
    this.index = index;
    this.message = message;
  }

  int getIndex() {
    return this.index;
  }
}
