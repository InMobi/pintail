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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.instrumentation.AbstractMessagingClientStatsExposer;
import com.inmobi.messaging.util.AuditUtil;

public class MockInMemoryConsumer extends AbstractMessageConsumer {

  private Map<String, BlockingQueue<Message>> source;

  public void setSource(Map<String, BlockingQueue<Message>> source) {
    this.source = source;
  }

  @Override
  public boolean isMarkSupported() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  protected AbstractMessagingClientStatsExposer getMetricsImpl() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void doMark() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  protected void doReset() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  protected Message getNext()
      throws InterruptedException, EndOfStreamException {
    BlockingQueue<Message> queue = source.get(topicName);
    if (queue == null) {
      queue = new LinkedBlockingQueue<Message>();
    }
    Message msg = queue.take();
    msg.set(AuditUtil.removeHeader(msg.getData().array()));
    return msg;
  }

  @Override
  public synchronized Message next()
      throws InterruptedException, EndOfStreamException {
    Message msg = getNext();
    return msg;
  }

  @Override
  public synchronized Message next(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException {
    Message msg = getNext(timeout, timeunit);
    return msg;
  }

  @Override
  public Long getPendingDataSize() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Message getNext(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException {
    BlockingQueue<Message> queue = source.get(topicName);
    if (queue == null) {
      queue = new LinkedBlockingQueue<Message>();
    }
    Message msg = queue.poll(timeout, timeunit);
    if (msg == null) {
      return null;
    }
    msg.set(AuditUtil.removeHeader(msg.getData().array()));
    return msg;
  }

}
