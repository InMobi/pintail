package com.inmobi.messaging.consumer;

import com.inmobi.messaging.ClientConfig;

/**
 * Abstract class implementing {@link MessageConsumer} interface.
 * 
 * It provides the access to configuration parameters({@link ClientConfig}) for
 * consumer interface
 *
 */
public abstract class AbstractMessageConsumer implements MessageConsumer {

  private ClientConfig config;

  /**
   * Initialize the consumer with passed configuration object
   * 
   * @param config {@link ClientConfig} for the consumer
   */
  protected void init(ClientConfig config) {
    this.config = config;
  }

  /**
   * Get the configuration of the consumer.
   * 
   * @return {@link ClientConfig} object
   */
  public ClientConfig getConfig() {
    return this.config;
  }
  
}
