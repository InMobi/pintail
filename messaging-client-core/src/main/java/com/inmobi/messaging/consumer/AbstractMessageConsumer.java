package com.inmobi.messaging.consumer;

import java.util.Date;

import com.inmobi.messaging.ClientConfig;

/**
 * Abstract class implementing {@link MessageConsumer} interface.
 * 
 * It provides the access to configuration parameters({@link ClientConfig}) for
 * consumer interface
 * 
 * It initializes topic name, consumer name and startTime. 
 * startTime is the time from which messages should be consumed. 
 * <ul>
 * <li>if no
 * startTime is passed, messages will be consumed from last marked position.
 * <li>
 * If there is no last marked position, messages will be consumed from the
 * starting of the available stream i.e. all messages that are not purged.
 * <li>If startTime or last marked position is beyond the retention
 * period of the stream, messages will be consumed from starting of the
 * available stream.
 *</ul>
 */
public abstract class AbstractMessageConsumer implements MessageConsumer {

  private ClientConfig config;
  protected String topicName;
  protected String consumerName;
  protected Date startTime;

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
   * consumerName and startTime.
   * 
   * @param topicName Name of the topic being consumed
   * @param consumerName Name of the consumer
   * @param startTime Starting time from which messages should be consumed
   * @param config {@link ClientConfig} for the consumer
   */
  public void init(String topicName, String consumerName, Date startTimestamp,
      ClientConfig config) {
    this.topicName = topicName;
    this.consumerName = consumerName;
    this.startTime = startTimestamp;
    init(config);
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
    init(topicName, consumerName, null, config);
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

  /**
   * Get the starting time of the consumption.
   * 
   * @return Date object
   */
  public Date getStartTime() {
    return startTime;
  }
}
