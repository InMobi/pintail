package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlternateTryLater;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestRetries {
  private NtMultiServer server;
  private ScribeMessagePublisher mb;

  @BeforeTest
  public void setUp() {
    server = TestServerStarter.getServer();
  }

  @AfterTest
  public void tearDown() {
    server.stop();
    if (mb != null)
      mb.close();
  }

  @Test()
  public void simpleSend() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7915;
      tserver = new NtMultiServer(new ScribeAlternateTryLater(), port);
      tserver.start();

      int timeoutSeconds = 10;
      mb = TestServerStarter.createPublisher(port, timeoutSeconds);

      String topic = "retry";
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.close();
      TimingAccumulator inspector = mb.getStats(topic);
      System.out.println("stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getRetryCount(), 1,
          "Retry not incremented");
      assertEquals(inspector.getSuccessCount(), 1,
          "success not incremented");
    } finally {
      tserver.stop();
    }
  }
}