package com.inmobi.messaging.publisher;

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
