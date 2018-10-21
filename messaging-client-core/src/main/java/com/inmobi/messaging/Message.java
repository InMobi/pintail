package com.inmobi.messaging;

/*
 * #%L
 * messaging-client-core
 * %%
 * Copyright (C) 2012 - 2014 InMobi
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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *  Message class holding the data.
 *
 */
public final class Message implements MessageBase {

  private ByteBuffer data;
  private Callback callback;
  private Map<String, Object> properties;

  public Message() {
  }

  /**
   * Create new message with {@link ByteBuffer}
   *
   * @param data The {@link ByteBuffer}
   */
  public Message(ByteBuffer data) {
    this.data = data;
  }

  /**
   * Create new message with byte array
   *
   * @param data The byte array.
   */
  public Message(byte[] data) {
    this.data = ByteBuffer.wrap(data);
  }

  /**
   * Create new message with byte array and a callback
   * @param data The byte array
   * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
   *        indicates no callback)
   */
  public Message(byte[] data, Callback callback) {
    this.data = ByteBuffer.wrap(data);
    this.callback = callback;
  }

  /**
   * Create new message with {@link ByteBuffer} and a callback
   *
   * @param data The {@link ByteBuffer}
   * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
   *        indicates no callback)
   */
  public Message(ByteBuffer data, Callback callback) {
    this.data = data;
    this.callback = callback;
  }

  /**
   * Get the data associated with message.
   *
   * @return {@link ByteBuffer} holding the data.
   */
  public ByteBuffer getData() {
    return data;
  }

  /**
   * Get the callback associated with message.
   *
   * @return the user-supplied callback
   */
  public Callback getCallback() {
    return callback;
  }

  /**
   * Get the Application property bag
   *
   * @return the application properties
   */
  public Map<String, Object> getProperties() {
    return properties;
  }

  public synchronized void set(ByteBuffer data) {
    this.data = data;
  }

  public synchronized void set(ByteBuffer data, Callback callback) {
    this.data = data;
    this.callback = callback;
  }

  public synchronized void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public synchronized void clear() {
    data.clear();
    callback = null;
    properties = null;
  }

  public long getSize() {
    return data.limit();
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, properties);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Message message = (Message) obj;
    return Objects.equals(data, message.data) && Objects.equals(properties, message.properties);
  }

  @Override
  public Message clone() {
    Message m = new Message(data.duplicate());
    if (properties != null) {
      m.setProperties(new HashMap<>(properties));
    }
    return m;
  }
}
