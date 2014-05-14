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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class SampleTestConsumer extends MockConsumer {

  public BlockingQueue<byte[]> messages = new LinkedBlockingQueue<byte[]>();

  private List<byte[]> createData() throws IOException, TException {

    Map<Long, Long> received = new HashMap<Long, Long>();
    long time = System.currentTimeMillis() - 60000;
    long window = time - time % 60000;
    received.put(window, 100L);
    AuditMessage packet = new AuditMessage(System.currentTimeMillis(),
        "testTopic", "publisher", "localhost", 1, received, received, null,
        null);
    TSerializer serializer = new TSerializer();
    // serializer.serialize(packet);
    byte[] output = serializer.serialize(packet);
    List<byte[]> result = new ArrayList<byte[]>();
    result.add(output);
    return result;
  }

  public void init(String topicName, String consumerName, Date startTimestamp,
      ClientConfig config) throws IOException {
    super.init(topicName, consumerName, startTimestamp, config);
    try {
      setMessages(createData());
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void setMessages(List<byte[]> messages) {
    this.messages.addAll(messages);
  }



  @Override
  protected Message getNext()
      throws InterruptedException, EndOfStreamException {
    return new Message(messages.take());
  }

}
