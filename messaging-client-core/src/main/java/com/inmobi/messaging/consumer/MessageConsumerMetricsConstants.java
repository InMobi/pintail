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

/**
 * Contains all the constants for Message consumer metrics
 */
public interface MessageConsumerMetricsConstants {
  public static final String TOPIC_CONTEXT = "topicName";
  public static final String CONSUMER_CONTEXT = "consumerName";
  public static final String MESSAGES_CONSUMED = "messagesConsumed";
  public static final String MARK_CALLS = "markCalls";
  public static final String RESET_CALLS = "resetCalls";
  public static final String TIMEOUTS_NEXT_CALL = "timeouts";
}
