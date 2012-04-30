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
  protected String topicName;
  protected String consumerName;

  /**
   * Initialize the consumer with passed configuration object
   * 
   * @param config {@link ClientConfig} for the consumer
   */
  protected void init(ClientConfig config) {
    this.config = config;
  }

  /**
   * Initialize the consumer with passed configuration object, streamName and 
   * consumerName.
   * 
   * @param topicName Name of the topic being consumed
   * @param consumerName Name of the consumer
   * @param config {@link ClientConfig} for the consumer
   */
  public void init(String topicName, String consumerName,
      ClientConfig config) {
    this.topicName = topicName;
    this.consumerName = consumerName;
    init(config);
  }

  /**
   * Get the configuration of the consumer.
   * 
   * @return {@link ClientConfig} object
   */
  public ClientConfig getConfig() {
    return this.config;
  }
  
  /**
   * Get the topic name being consumed.
   * 
   * @return String topicName
   */
  public String getTopicName() {
    return topicName;
  }

  /**
   * Get the consumer name
   * 
   * @return String consumerName
   */
  public String getConsumerName() {
    return consumerName;
  }

}
