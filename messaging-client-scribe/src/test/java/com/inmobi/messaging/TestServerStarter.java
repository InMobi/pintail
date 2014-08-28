package com.inmobi.messaging;

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

import static org.testng.Assert.assertNotNull;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import random.pkg.NtMultiServer;

import com.inmobi.messaging.netty.ScribeBlockingMessagePublisher;
import com.inmobi.messaging.netty.ScribeMessagePublisher;
import com.inmobi.messaging.netty.ScribePublisherConfiguration;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;

public class TestServerStarter {
  private static NtMultiServer server;

  @BeforeSuite
  public void setUp() {
    safeInit();
  }

  @AfterSuite
  public void tearDown() {
    server.stop();
  }

  public static NtMultiServer getServer() {
    safeInit();
    assertNotNull(server);
    return server;
  }

  private static synchronized void safeInit() {
    if (server == null) {
      server = new NtMultiServer();
    }
  }

  public static final int port = PortNumberUtil.getFreePortNumber(7912);

  public static ScribeMessagePublisher createPublisher(final int port,
      final int timeout) throws Exception {
    return createPublisher(port, timeout, 5);
  }

  public static ScribeMessagePublisher createPublisher(final int port,
      final int timeout, final int backOff) throws Exception {
    return createPublisher(port, timeout, backOff, true, true);
  }

  public static ScribeMessagePublisher createPublisher(final int port,
      final int timeout, final int backOff, final boolean enableRetries,
      final boolean resendOnAckLost) throws Exception {
    return createPublisher(port, timeout, backOff, enableRetries,
        resendOnAckLost, 100, 100);
  }

  public static ScribeMessagePublisher createPublisher(final int port,
      final int timeout, final int backOff, final boolean enableRetries,
      final boolean resendOnAckLost, final int msgQueueSize,
      final int ackQueueSize) throws Exception {
    return createPublisher(port, timeout, backOff, enableRetries,
        resendOnAckLost, msgQueueSize, ackQueueSize, -1);
  }

  public static ScribeMessagePublisher createPublisher(final int port,
      final int timeout, final int backOff, final boolean enableRetries,
      final boolean resendOnAckLost, final int msgQueueSize,
      final int ackQueueSize, final boolean useBlockingPublisher)
      throws Exception {
    return createPublisher(port, timeout, backOff, enableRetries,
        resendOnAckLost, msgQueueSize, ackQueueSize, -1, useBlockingPublisher);
  }

  public static ScribeMessagePublisher createPublisher(final int port,
      final int timeout, final int backOff, final boolean enableRetries,
      final boolean resendOnAckLost, final int msgQueueSize,
      final int ackQueueSize, final int numRetries) throws Exception {
    return createPublisher(port, timeout, backOff, enableRetries,
        resendOnAckLost, msgQueueSize, ackQueueSize, numRetries, false);
  }

  public static ScribeMessagePublisher createPublisher(final int port,
      final int timeout, final int backOff, final boolean enableRetries,
      final boolean resendOnAckLost, final int msgQueueSize,
      final int ackQueueSize, final int numRetries,
      final boolean useBlockingPublisher) throws Exception {
    ScribeMessagePublisher pub;
    if (useBlockingPublisher) {
      pub = new ScribeBlockingMessagePublisher();
    } else {
      pub = new ScribeMessagePublisher();
    }
    ClientConfig config = new ClientConfig();
    config.set(ScribePublisherConfiguration.hostNameConfig, "localhost");
    config.set(ScribePublisherConfiguration.portConfig, port + "");
    config.set(ScribePublisherConfiguration.backOffSecondsConfig, backOff + "");
    config.set(ScribePublisherConfiguration.timeoutSecondsConfig, timeout + "");
    config.set(ScribePublisherConfiguration.retryConfig, enableRetries + "");
    config.set(ScribePublisherConfiguration.resendAckLostConfig,
        resendOnAckLost + "");
    config.set(ScribePublisherConfiguration.asyncSenderSleepMillis, "10");
    config.set(ScribePublisherConfiguration.messageQueueSizeConfig,
        msgQueueSize + "");
    config.set(ScribePublisherConfiguration.ackQueueSizeConfig, ackQueueSize
        + "");
    config.set(ScribePublisherConfiguration.drainRetriesOnCloseConfig,
        numRetries + "");
    config.set(AbstractMessagePublisher.AUDIT_ENABLED_KEY, "true");
    pub.init(config);
    return pub;
  }

  public static ScribeMessagePublisher createPublisher() throws Exception {
    return createPublisher(port, 5);
  }
}
