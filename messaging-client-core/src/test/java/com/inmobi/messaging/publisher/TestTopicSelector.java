package com.inmobi.messaging.publisher;

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
    MsgValueTopicSelector selector = (MsgValueTopicSelector)
        TopicSelector.create(topic, conf);
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
