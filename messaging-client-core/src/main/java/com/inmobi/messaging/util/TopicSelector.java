package com.inmobi.messaging.util;

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

import com.inmobi.messaging.ClientConfig;

@SuppressWarnings("rawtypes")
public abstract class TopicSelector<T> {

  private static final String CLASS_SUFFIX = ".selector.class";

  /*
   * Selects the topic to publish for a given message.
   */
  public abstract String selectTopic(T message);

  /*
   * Creates the TopicSelector for a given logical topic. If no topic selector
   * set for the class then return the default selector which selects same
   * topic as the logical topic.
   */
  public static TopicSelector create(String logicalTopic) {
    return create(logicalTopic, new ClientConfig());
  }

  @SuppressWarnings("unchecked")
  public static TopicSelector create(String logicalTopic, ClientConfig conf) {
    String name = conf.getString(logicalTopic + CLASS_SUFFIX);
    TopicSelector selector;
    if (name != null) {
      Class<TopicSelector> claz;
      try {
        claz = (Class<TopicSelector>) Class.forName(name);
        selector = claz.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      selector = new DefaultTopicSelector();
    }
    selector.init(logicalTopic, conf);
    return selector;
  }

  protected void init(String logicalTopic, ClientConfig conf) {
  }

  public void close() {
  }

  public static void setSelectorClass(ClientConfig conf, String logicalTopic,
      String classname) {
    conf.set(logicalTopic + CLASS_SUFFIX, classname);
  }

  public static class DefaultTopicSelector extends TopicSelector {
    private String topic;
    protected ClientConfig conf;

    public DefaultTopicSelector() { }

    @Override
    protected void init(String logicalTopic, ClientConfig conf) {
      this.topic = logicalTopic;
      this.conf = conf;
    }

    @Override
    public String selectTopic(Object object) {
      return topic;
    }
  }

}
