package com.inmobi.messaging.consumer;

/*
 * #%L
 * messaging-client-core
 * %%
 * Copyright (C) 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
   * @throws InterruptedException if interrupted while waiting for Message
   * @throws EndOfStreamException When consumer consumed all messages in the
   *  stream
   */
  public Message next() throws InterruptedException, EndOfStreamException;

  /**
   * Reads the next message if it is available with in the specified timeout.
   *
   * @return {@link Message} object if available within timeout
   *         Null otherwise
   * @throws InterruptedException if interrupted while waiting for Message
   * @throws EndOfStreamException When consumer consumed all messages in the
   *  stream
   */
  public Message next(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException;

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
   * @throws IOException
   */
  public void reset() throws IOException;

  /**
   * Close and cleanup all resources such as connection, file handles etc.
   */
  public void close();
}
