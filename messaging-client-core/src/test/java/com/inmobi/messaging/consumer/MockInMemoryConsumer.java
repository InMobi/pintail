package com.inmobi.messaging.consumer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.ConsumerUtil;

public class MockInMemoryConsumer extends AbstractMessageConsumer {

  private Map<String, BlockingQueue<Message>> source;
  private int offset = 0;
  private static final byte[] magicBytes = { (byte) 0xAB, (byte) 0xCD,
      (byte) 0xEF };
  private static final byte[] versions = { 1 };
  private static final int HEADER_LENGTH = 16;

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
  protected Message getNext() throws InterruptedException {
    BlockingQueue<Message> queue = source.get(topicName);
    if (queue == null)
      queue = new LinkedBlockingQueue<Message>();
    Message msg = queue.take();
    msg.set(ConsumerUtil.removeHeader(msg.getData().array()));
    return msg;
  }

  public synchronized Message next() throws InterruptedException {
    Message msg = getNext();
    return msg;
  }




}
