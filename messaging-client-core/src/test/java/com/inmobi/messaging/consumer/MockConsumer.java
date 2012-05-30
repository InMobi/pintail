package com.inmobi.messaging.consumer;

import java.io.IOException;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class MockConsumer extends AbstractMessageConsumer {

  public static String mockMsg = "mock msg";
  boolean initedConf = false;
  
  protected void init(ClientConfig config) throws IOException {
    super.init(config);
    initedConf = true;
  }
  @Override
  public Message next() throws InterruptedException {
      return new Message(mockMsg.getBytes());
  }

  @Override
  public boolean isMarkSupported() {
    return false;
  }

  @Override
  public void mark() throws IOException {
    
  }

  @Override
  public void reset() throws IOException {
    
  }

  @Override
  public void close() {
    
  }

}
