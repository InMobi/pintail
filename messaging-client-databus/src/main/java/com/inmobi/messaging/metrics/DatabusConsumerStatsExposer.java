package com.inmobi.messaging.metrics;

/*
 * #%L
 * messaging-client-databus
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

import java.util.Map;

import com.inmobi.messaging.consumer.BaseMessageConsumerStatsExposer;

public class DatabusConsumerStatsExposer extends
    BaseMessageConsumerStatsExposer {

  public static String CONSUMER_NUMBER_CONTEXT = "consumerNumber";

  Integer consumerNumber;
  public DatabusConsumerStatsExposer(String topicName, String consumerName,
      int consumerNumber) {
    super(topicName, consumerName);
    this.consumerNumber = consumerNumber;
  }

  protected void addToContextsMap(Map<String, String> contexts) {
    super.addToContextsMap(contexts);
    contexts.put(CONSUMER_NUMBER_CONTEXT, consumerNumber.toString());
  }
}
