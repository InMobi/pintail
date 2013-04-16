package com.inmobi.messaging.consumer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.inmobi.messaging.Message;

/**
 * Interface for consuming a message stream.
 * 
 */
public interface MessageConsumer {

  /**
   * Reads the next message
   * 
   * It is a blocking call which waits for the message to be available on the
   * stream
   * 
   * @return {@link Message} object
   * 
   * @throws InterruptedException 
   */
  public Message next() throws InterruptedException;
  
  /**
   * Reads the next message if it is available with in the specified timeout.
   * Otherwise returns null
   * @throws InterruptedException
   */
  public Message next(long timeout, TimeUnit timeunit) 
      throws InterruptedException;

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
   * 
   * @throws IOException 
   */
  public void mark() throws IOException;

  /**
   * Reset to last the marked position
   * 
   * @throws IOException 
   * @throws Exception 
   */
  public void reset() throws Exception;

  /**
   * Close and cleanup all resources such as connection, file handles etc.
   */
  public void close();
}
