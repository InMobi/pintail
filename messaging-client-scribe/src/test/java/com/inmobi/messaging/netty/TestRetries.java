package com.inmobi.messaging.netty;

import static org.testng.Assert.assertEquals;

import org.testng.Assert;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlternateTryLater;
import random.pkg.ScribeAlwaysTryAgain;
import random.pkg.ScribeSlackOnce;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.TestServerStarter;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestRetries {
  @Test
  public void simpleSend() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7901;
      tserver = new NtMultiServer(new ScribeAlternateTryLater(), port);
      tserver.start();

      int timeoutSeconds = 10;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds);

      String topic = "retry";
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.close();
      TimingAccumulator inspector = mb.getStats(topic);
      System.out.println("TestRetries.simpleSend stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getRetryCount(), 1,
          "Retry not incremented");
      assertEquals(inspector.getSuccessCount(), 1,
          "success not incremented");
    } finally {
      tserver.stop();
    }
    System.out.println("TestRetries.simpleSend done");
  }

  @Test
  public void testEnableRetries() throws Exception {
    testEnableRetries(true, 7902);
    testEnableRetries(false, 7902);
  }

  public void testEnableRetries(boolean enableRetries, int port)
      throws Exception {
    NtMultiServer tserver = null;
    try {
      tserver = new NtMultiServer(new ScribeAlternateTryLater(), port);
      tserver.start();

      int timeoutSeconds = 10;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, enableRetries, true);

      String topic = "retry";
      mb.publish(topic, new Message("mmmm".getBytes()));
      TimingAccumulator inspector = mb.getStats(topic);
      // if retry is disabled, ensure that message is acked before closing
      // the publisher.
      if (!enableRetries) {
        while (inspector.getInFlight() != 0) {
          Thread.sleep(1);
        }
      }
      mb.close();
      
      System.out.println("testEnableRetries:" + enableRetries + " stats:" 
          + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      if (enableRetries) {
        assertEquals(inspector.getRetryCount(), 1,
            "Retry not incremented");
        assertEquals(inspector.getSuccessCount(), 1,
            "success not incremented");
      } else {
        assertEquals(inspector.getGracefulTerminates(), 1,
            "Graceful terminates not incremented");
        assertEquals(inspector.getSuccessCount(), 0,
            "success incremented");
      }
    } finally {
      tserver.stop();
    }
    System.out.println("TestRetries.testEnableRetries:" + enableRetries  +
    		" done");
  }

  @Test
  public void testAlwaysTryAgain() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7903;
      tserver = new NtMultiServer(new ScribeAlwaysTryAgain(), port);
      tserver.start();

      int timeoutSeconds = 10;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, true, 100, 100, 10);

      String topic = "retry";
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.close();
      TimingAccumulator inspector = mb.getStats(topic);
      System.out.println("testAlwaysTryAgain stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      Assert.assertTrue(inspector.getRetryCount() > 0,
          "Retry not incremented");
      assertEquals(inspector.getLostCount(), 1,
          "Lost not incremented");
      assertEquals(inspector.getSuccessCount(), 0,
          "success incremented");
    } finally {
      tserver.stop();
    }
    System.out.println("TestRetries.testAlwaysTryAgain done");
  }

  @Test
  public void testResendOnAckLost() throws Exception {
    testResendOnAckLost(true);
    testResendOnAckLost(false);
  }
  
  public void testResendOnAckLost(boolean resendOnAckLost) throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = 7904;
      tserver = new NtMultiServer(new ScribeSlackOnce(), port);
      tserver.start();

      int timeoutSeconds = 200;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, resendOnAckLost);

      String topic = "resend";
      // publish a message and suggest reconnect to the server
      // ack will be lost for the message.
      mb.publish(topic, new Message("msg1".getBytes()));
      mb.getTopicPublisher(topic).suggestReconnect();
      Thread.sleep(1000);
      mb.publish(topic, new Message("msg2".getBytes()));
      mb.close();
      TimingAccumulator inspector = mb.getStats(topic);
      System.out.println("testResendOnAckLost " + resendOnAckLost + " stats:" 
        + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      if (resendOnAckLost) {
        assertEquals(inspector.getSuccessCount(), 2,
            "success not incremented");
      } else {
        assertEquals(inspector.getSuccessCount(), 1,
            "success not incremented");
        assertEquals(inspector.getGracefulTerminates(), 1,
            "Graceful terminates not incremented");
      }
    } finally {
      tserver.stop();
    }
    System.out.println("TestRetries.testResendOnAckLost:" + resendOnAckLost  +
        " done");
  }
}
