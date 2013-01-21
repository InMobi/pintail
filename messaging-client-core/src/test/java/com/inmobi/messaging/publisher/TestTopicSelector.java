package com.inmobi.messaging.publisher;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.util.TopicSelector;

public class TestTopicSelector {

  @Test
  public void testDefaultSelector() throws IOException {
    ClientConfig conf = new ClientConfig();
    String topic = "logicalTopic";
    String msg = "msg1";
    TopicSelector selector = TopicSelector.create(topic, conf);
    Assert.assertEquals(selector.selectTopic(msg), topic);
    selector.close();
  }

  @Test
  public void test() throws IOException {
    ClientConfig conf = new ClientConfig();
    String topic = "logicalTopic";
    String msg = "msg1";
    TopicSelector.setSelectorClass(conf, topic, 
        MsgValueTopicSelector.class.getName());
    MsgValueTopicSelector selector = (MsgValueTopicSelector) TopicSelector.create(topic, conf);
    Assert.assertEquals(selector.selectTopic(msg), msg);
    Assert.assertEquals(selector.getLogicalTopic(), topic);
    selector.close();
  }

  //selects the topic based on the message string
  public static class MsgValueTopicSelector extends TopicSelector<String> {
    private String logicalTopic;
    
    @Override
    protected void init(String logicalTopic, ClientConfig conf) {
      this.logicalTopic = logicalTopic;
    }
    
    @Override
    public String selectTopic(String object) {
      return object;
    }
    
    public String getLogicalTopic() {
      return logicalTopic;
    }
  }
}
