package com.inmobi.messaging.publisher.examples;

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
      System.err.println("Usage: TopicSelectorExample" +
          " <topic> <maxSeq>");
      return;
    }
    AbstractMessagePublisher publisher = 
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    String top = args[0];
    String top1 = top + "1";
    String top2 = top + "2";
    long maxSeq = Integer.parseInt(args[1]);

    TopicSelector.setSelectorClass(conf, top, MsgValueTopicSelector.class.getName());
    MsgValueTopicSelector selector = (MsgValueTopicSelector)TopicSelector.create(top, conf);

    for (long seq = 1; seq <= maxSeq; seq++) {
      String str1 = Long.toString(seq);
      TopicMessage msg1 = new TopicMessage(top1,str1);
      TopicMessage msg2 = new TopicMessage(top2,str1);

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
    publisher.close();
    System.out.println("Total topic invocations: " + 
        publisher.getStats(top1).getInvocationCount());
    System.out.println("Total topic success: " +
        publisher.getStats(top1).getSuccessCount());
    System.out.println("Total topic unhandledExceptions: " +
        publisher.getStats(top1).getUnhandledExceptionCount());

    System.out.println("Total topic invocations: " + 
        publisher.getStats(top2).getInvocationCount());
    System.out.println("Total topic success: " +
        publisher.getStats(top2).getSuccessCount());
    System.out.println("Total topic unhandledExceptions: " +
        publisher.getStats(top2).getUnhandledExceptionCount());
  }

  public static class MsgValueTopicSelector extends TopicSelector<TopicMessage> {
    @Override
    public String selectTopic(TopicMessage object) {
      return object.getTopic();
    }
  }
}

class TopicMessage
{
  String topic;
  String message;
  TopicMessage(String topic, String message){
    this.topic = topic;
    this.message = message;
  }

  String getTopic(){
    return this.topic;
  }
}
