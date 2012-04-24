package com.inmobi.messaging.consumer;

import com.inmobi.messaging.ClientConfig;

public abstract class AbstractMessageConsumer implements MessageConsumer {

  private ClientConfig config;

  protected void init(ClientConfig config) {
    this.config = config;
    start();
  }

  protected abstract void start();

  public ClientConfig getConfig() {
    return this.config;
  }
}
