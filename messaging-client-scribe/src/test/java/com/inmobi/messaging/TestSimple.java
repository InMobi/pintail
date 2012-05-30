package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.apache.thrift.TException;
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
    
    publisher = TestServerStarter.createPublisher();
    TimingAccumulator inspector = publisher.getStats();
    long success = inspector.getSuccessCount();
    publisher.publish("ch", new Message("mmmm".getBytes()));
    
    // Wait for all operations to complete
    while (inspector.getInFlight() != 0) {
      Thread.sleep(100);
    }
    assertEquals(inspector.getSuccessCount(), success + 1);
  }

}
