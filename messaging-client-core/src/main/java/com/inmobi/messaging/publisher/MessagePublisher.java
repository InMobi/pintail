package com.inmobi.messaging.publisher;

import com.inmobi.messaging.Message;

/**
 * Publishes message to the configured concrete MessagePublisher.
 *
 */
public interface MessagePublisher {

  /**
   * Publishes the message onto the configured concrete MessagePublisher.
   * 
   * This method should return very fast and do most of its processing 
   * asynchronously.
   *
   * @param topicName The topic on which message should be published
   * @param m The {@link Message} object to be published
   */
  public void publish(String topicName, Message m);

  /**
   * Closes and cleans up any connections, file handles etc.
   * 
   */
  public void close();
}
