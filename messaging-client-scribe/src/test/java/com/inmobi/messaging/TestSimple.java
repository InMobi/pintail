package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestSimple {
  private NtMultiServer server;
  private ScribeMessagePublisher publisher;

  @BeforeTest
  public void setUp() {
    server = TestServerStarter.getServer();
  }

  @AfterTest
  public void tearDown() {
    server.stop();
    if (publisher != null)
      publisher.close();
  }

  @Test()
  public void simpleSend() throws Exception {
    server.start();
    runTest();
    //create the publisher again
    runTest();
  }

  private void sendMessages() throws Exception {
    String topic1 = "test1";
    String topic2 = "test2";
    publisher.publish(topic1, new Message("msg1".getBytes()));
    publisher.publish(topic2, new Message("msg2".getBytes()));
    TimingAccumulator inspector1 = publisher.getStats(topic1);
    TimingAccumulator inspector2 = publisher.getStats(topic1);
    // Wait for all operations to complete
    while (inspector1.getInFlight() != 0) {
      Thread.sleep(100);
    }
    while (inspector2.getInFlight() != 0) {
      Thread.sleep(100);
    }
    assertEquals(inspector1.getSuccessCount(), 1);
    assertEquals(inspector2.getSuccessCount(), 1);
  }

  private void runTest() throws Exception {
    publisher = TestServerStarter.createPublisher();
    sendMessages();
    publisher.close();
  }
}
