package com.inmobi.messaging;

/**
 * Publishes message to the configured concrete MessagePublisher.
 *
 */
public interface MessagePublisher {

  /*
   * Publishes the message onto the configured concrete MessagePublisher.
   * This method should return very fast and do most of its processing 
   * asynchronously.
   */
  void publish(Message m);

  /*
   * Closes and cleans up any connections, file handles etc.
   */
  void close();
}
