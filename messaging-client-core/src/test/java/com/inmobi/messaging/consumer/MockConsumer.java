package com.inmobi.messaging.consumer;

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
import java.util.concurrent.TimeUnit;

import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class MockConsumer extends AbstractMessageConsumer {

  public static String mockMsg = "mock msg";
  boolean initedConf = false;
  public static boolean block = false;

  protected void init(ClientConfig config) throws IOException {
    super.init(config);
    initedConf = true;
  }
  @Override
  protected Message getNext() throws InterruptedException, EndOfStreamException {
    if (block) {
      Thread.sleep(2000);
    }
    return new Message(mockMsg.getBytes());
  }

  @Override
  public boolean isMarkSupported() {
    return false;
  }

  @Override
  public void close() { }

  @Override
  protected AbstractMessagingClientStatsExposer getMetricsImpl() {
    return new BaseMessageConsumerStatsExposer(topicName, consumerName);
  }

  @Override
  protected void doMark() throws IOException {
  }

  @Override
  protected void doReset() throws IOException { }

  @Override
  protected Message getNext(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException {
    return null;
  }

}
