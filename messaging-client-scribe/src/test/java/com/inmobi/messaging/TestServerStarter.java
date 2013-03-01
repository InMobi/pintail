package com.inmobi.messaging;

import static org.testng.Assert.assertNotNull;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import random.pkg.NtMultiServer;

import com.inmobi.messaging.ClientConfig;
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

  public static final int port = 7912;

  public static ScribeMessagePublisher createPublisher(int port, int timeout)
      throws Exception {
    return createPublisher(port, timeout, 5);
  }

  public static ScribeMessagePublisher createPublisher(int port, int timeout,
      int backOff) throws Exception {
    return createPublisher(port, timeout, backOff, true, true);
  }

  public static ScribeMessagePublisher createPublisher(int port, int timeout,
      int backOff, boolean enableRetries, boolean resendOnAckLost)
      throws Exception {
    return createPublisher(port, timeout, backOff, enableRetries,
        resendOnAckLost, 100, 100);
  }

  public static ScribeMessagePublisher createPublisher(int port, int timeout,
      int backOff, boolean enableRetries, boolean resendOnAckLost,
      int msgQueueSize, int ackQueueSize) throws Exception {
    return createPublisher(port, timeout, backOff, enableRetries,
        resendOnAckLost, msgQueueSize, ackQueueSize, -1);
  }

  public static ScribeMessagePublisher createPublisher(int port, int timeout,
      int backOff, boolean enableRetries, boolean resendOnAckLost,
      int msgQueueSize, int ackQueueSize, int numRetries) throws Exception {
    ScribeMessagePublisher pub = new ScribeMessagePublisher();
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
