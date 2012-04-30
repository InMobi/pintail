package com.inmobi.messaging.consumer;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class MockConsumer extends AbstractMessageConsumer {

  public static String mockMsg = "mock msg";
  boolean initedConf = false;
  
  protected void init(ClientConfig config) {
    super.init(config);
    initedConf = true;
  }
  @Override
  public Message next() {
      return new Message(mockMsg.getBytes());
  }

  @Override
  public boolean isMarkSupported() {
    return false;
  }

  @Override
  public void mark() {
    
  }

  @Override
  public void reset() {
    
  }

  @Override
  public void close() {
    
  }

}
