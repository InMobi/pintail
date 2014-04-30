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

import java.util.HashMap;
import java.util.Map;

import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.Message;

public class MockPublisher extends AbstractMessagePublisher {
  private static Map<String, Message> msgs = new HashMap<String, Message>();

  public static void reset() {
    msgs.clear();
  }

  public static void reset(String topic) {
    msgs.remove(topic);
  }

  public static Message getMsg(String topic) {
    return msgs.get(topic);
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    String topic = headers.get(HEADER_TOPIC);
    msgs.put(topic, m);
    getStats(topic).accumulateOutcomeWithDelta(Outcome.SUCCESS, 0);
  }
}
