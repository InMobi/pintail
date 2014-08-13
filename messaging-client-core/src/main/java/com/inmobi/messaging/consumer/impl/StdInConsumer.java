package com.inmobi.messaging.consumer.impl;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;
import com.inmobi.messaging.consumer.EndOfStreamException;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.BaseMessageConsumerStatsExposer;
import com.inmobi.messaging.instrumentation.AbstractMessagingClientStatsExposer;

/**
 * Stdin consumer reads messages from stdin.
 */
public class StdInConsumer extends AbstractMessageConsumer {

  BufferedReader in;
  String topicName;

  protected void init(ClientConfig config) throws IOException {
    super.init(config);
    topicName = getConfig().getString("stdin.topic");
    start();
  }

  @Override
  public Message getNext() throws InterruptedException, EndOfStreamException {
    try {
      String str = in.readLine();
      if (str != null) {
        return new Message(str.getBytes());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public boolean isMarkSupported() {
    return false;
  }

  @Override
  public void doMark() throws IOException {
    // nothing to commit
  }

  @Override
  public void doReset() throws IOException {
    // nothing to rollback
  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected void start() {
    in = new BufferedReader(new InputStreamReader(System.in));
  }

  public static void main(String[] args) throws Exception {
    ClientConfig config = new ClientConfig();
    config.set("stdin.topic", "mytopic");
    config.set(MessageConsumerFactory.CONSUMER_CLASS_NAME_KEY,
        StdInConsumer.class.getName());
    MessageConsumer consumer = MessageConsumerFactory.create(config);

    try {
      while (true) {
        Message msg = consumer.next();
        System.out.println("Message:" + msg.getData());
      }
    }  finally {
      consumer.close();
    }
  }

  @Override
  protected AbstractMessagingClientStatsExposer getMetricsImpl() {
    return new BaseMessageConsumerStatsExposer(topicName, consumerName);
  }

  @Override
  protected Message getNext(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException {
    return null;
  }
}
