package com.inmobi.messaging.consumer;

import com.inmobi.messaging.Message;

/**
 * Interface for consuming a message stream.
 * 
 */
public interface MessageConsumer {

  /**
   * Reads the next message
   * 
   * It is synchronous call which waits for the message to be available on the
   * stream
   * 
   * @return {@link Message} object
   */
  public Message next();

  /**
   * Tells if this interface supports <code>mark</code> and <code>reset</code>
   * methods 
   * 
   * @return boolean The value <code>true</code> mark and reset are supported
   *                 <code>false</code> otherwise. 
   */
  public boolean isMarkSupported();
  
  /**
   * Mark the position in the stream up to last read message.
   */
  public void mark();

  /**
   * Reset to last the marked position
   */
  public void reset();

  /**
   * Close and cleanup all resources such as connection, file handles etc.
   */
  public void close();
}
