package com.inmobi.messaging;

/**
 * Publishes message to the configured concrete MessagePublisher.
 *
 */
public interface MessagePublisher {

  /*
   * Initializes the MessagePublisher instance
   */
  void init(ClientConfig config);

  /*
   * Closes and cleans up any connections, file handles etc.
   */
  void close();

  /*
   * Publishes the message onto the configured concrete MessagePublisher.
   * This method should return very fast and do most of its processing 
   * asynchronously.
   */
  void publish(Message m);
}
