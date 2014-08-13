package com.inmobi.messaging.netty;

/*
 * #%L
 * messaging-client-scribe
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;

public class ScribeMessagePublisher extends AbstractMessagePublisher implements
    ScribePublisherConfiguration {
  public static final Log LOG = LogFactory.getLog(ScribeMessagePublisher.class);

  private String host;
  private int port;
  private int backoffSeconds;
  private int timeoutSeconds;
  private long asyncSleepInterval;
  private boolean resendOnAckLost;
  private boolean enableRetries;
  private int msgQueueSize;
  private int ackQueueSize;
  private int numDrainsOnClose;

  protected Map<String, ScribeTopicPublisher> scribeConnections =
      new HashMap<String, ScribeTopicPublisher>();

  @Override
  public void init(ClientConfig config) throws IOException {
    super.init(config);
    init(config.getString(hostNameConfig, DEFAULT_HOST), config.getInteger(
        portConfig, DEFAULT_PORT), config.getInteger(backOffSecondsConfig,
        DEFAULT_BACKOFF), config.getInteger(timeoutSecondsConfig,
        DEFAULT_TIMEOUT),
        config.getBoolean(retryConfig, DEFAULT_ENABLE_RETRIES),
        config.getBoolean(resendAckLostConfig, DEFAULT_RESEND_ACKLOST),
        config.getLong(asyncSenderSleepMillis, DEFAULT_ASYNC_SENDER_SLEEP),
        config.getInteger(messageQueueSizeConfig, DEFAULT_MSG_QUEUE_SIZE),
        config.getInteger(ackQueueSizeConfig, DEFAULT_ACK_QUEUE_SIZE),
        config
            .getInteger(drainRetriesOnCloseConfig, DEFAULT_NUM_DRAINS_ONCLOSE));
  }

  private void init(String host, int port, int backoffSeconds, int timeout,
      boolean enableRetries, boolean resendOnAckLost, long sleepInterval,
      int msgQueueSize, int ackQueueSize, int numDrainsOnClose)
      throws IOException {
    this.host = host;
    this.port = port;
    this.backoffSeconds = backoffSeconds;
    this.timeoutSeconds = timeout;
    this.enableRetries = enableRetries;
    this.resendOnAckLost = resendOnAckLost;
    this.asyncSleepInterval = sleepInterval;
    this.msgQueueSize = msgQueueSize;
    this.ackQueueSize = ackQueueSize;
    this.numDrainsOnClose = numDrainsOnClose;
    LOG.info("Initialized ScribeMessagePublisher with host:" + host + " port:"
        + +port + " backoffSeconds:" + backoffSeconds + " timeoutSeconds:"
        + timeoutSeconds + " enableRetries:" + enableRetries
        + " resendOnAckLost:" + resendOnAckLost + "asyncSleepInterval:"
        + asyncSleepInterval + "msgQueueSize:" + msgQueueSize + "ackQueueSize:"
        + ackQueueSize + "numDrainsOnClose:" + numDrainsOnClose);
  }

  protected void initTopic(String topic, TimingAccumulator stats) {
    super.initTopic(topic, stats);
    if (scribeConnections.get(topic) == null) {
      ScribeTopicPublisher connection = new ScribeTopicPublisher();
      scribeConnections.put(topic, connection);
      initConnection(topic, connection, stats);
    }
  }
  
  protected void initConnection(String topic, ScribeTopicPublisher connection, TimingAccumulator stats) {
    connection.init(topic, host, port, backoffSeconds, timeoutSeconds, stats,
      enableRetries, resendOnAckLost, asyncSleepInterval, msgQueueSize,
      ackQueueSize, numDrainsOnClose);
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    String topic = headers.get(HEADER_TOPIC);
    scribeConnections.get(topic).publish(m);
  }

  @Override
  protected void closeTopic(String topicName) {
    ScribeTopicPublisher scribePublisher = scribeConnections.get(topicName);
    if (scribePublisher == null) {
      LOG.warn("Close called on topic[" + topicName + "]"
          + " for which ScribeTopicPublisher doesn't exist");
      return;
    }
    scribePublisher.close();
  }

  ScribeTopicPublisher getTopicPublisher(String topicName) {
    return scribeConnections.get(topicName);
  }
}
