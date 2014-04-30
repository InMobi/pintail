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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.inmobi.messaging.Message;

public class MockInMemoryPublisher extends AbstractMessagePublisher {

  public Map<String, BlockingQueue<Message>> source =
      new HashMap<String, BlockingQueue<Message>>();

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    // TODO Auto-generated method stub
    String topic = headers.get(AbstractMessagePublisher.HEADER_TOPIC);
    if (source.get(topic) == null) {
      BlockingQueue<Message> list = new LinkedBlockingQueue<Message>();
      list.add(m);
      source.put(topic, list);
    } else {
      source.get(topic).add(m);
    }

  }

  public void reset() {
    source = new HashMap<String, BlockingQueue<Message>>();
  }

}
