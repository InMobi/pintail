package com.inmobi.messaging.netty;

/*
 * #%L
 * messaging-client-scribe
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

import com.inmobi.messaging.instrumentation.PintailTimingAccumulator;

/**
 * Blocks the thread publishing the message till the time queue gets space to
 * add the message.
 */
public class ScribeBlockingMessagePublisher extends ScribeMessagePublisher {
  @Override
  protected void initTopic(final String topic, final PintailTimingAccumulator stats) {
    if (scribeConnections.get(topic) == null) {
      ScribeTopicPublisher connection = new ScribeBlockingTopicPublisher();
      scribeConnections.put(topic, connection);
      initConnection(topic, connection, stats);
    }
    super.initTopic(topic, stats);
  }
}
