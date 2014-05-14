package com.inmobi.messaging.publisher;

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
import com.inmobi.instrumentation.TimingAccumulator;

public class TopicStatsExposer extends AbstractMessagingClientStatsExposer {

  public static final String STATS_TYPE_CONTEXT_NAME = "messaging_type";
  public static final String TOPIC_CONTEXT_NAME = "category";
  public static final String STATS_TYPE = "application";

  final String topic;
  final TimingAccumulator stats;
  public TopicStatsExposer(String topicName,
      TimingAccumulator timingAccumulator) {
    this.stats = timingAccumulator;
    this.topic = topicName;
  }

  public TimingAccumulator getTimingAccumulator() {
    return stats;
  }

  @Override
  protected void addToStatsMap(Map<String, Number> map) {
    map.putAll(stats.getMap());
  }

  @Override
  protected void addToContextsMap(Map<String, String> map) {
    map.put(STATS_TYPE_CONTEXT_NAME, STATS_TYPE);
    map.put(TOPIC_CONTEXT_NAME, topic);
  }
}
