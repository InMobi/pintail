package com.inmobi.messaging.consumer;

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

import java.util.Map;

import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;

public abstract class AbstractMessageConsumerStatsExposer extends
    AbstractMessagingClientStatsExposer implements
    MessageConsumerMetricsConstants {

  private final String topicName;
  private final String consumerName;

  protected AbstractMessageConsumerStatsExposer(String topicName,
      String consumerName) {
    this.topicName = topicName;
    this.consumerName = consumerName;
  }

  protected void addToContextsMap(Map<String, String> contexts) {
    contexts.put(TOPIC_CONTEXT, topicName);
    contexts.put(CONSUMER_CONTEXT, consumerName);
  }
}
