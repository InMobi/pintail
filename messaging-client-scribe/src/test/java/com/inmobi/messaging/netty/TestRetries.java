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

import static org.testng.Assert.assertEquals;

import org.testng.Assert;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlternateTryLater;
import random.pkg.ScribeAlwaysTryAgain;
import random.pkg.ScribeSlackOnce;

import com.inmobi.instrumentation.PintailTimingAccumulator;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.PortNumberUtil;
import com.inmobi.messaging.TestServerStarter;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestRetries {
  @Test
  public void simpleSend() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7901);
      tserver = new NtMultiServer(new ScribeAlternateTryLater(), port);
      tserver.start();

      int timeoutSeconds = 10;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds);

      String topic = "retry";
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.close();
      PintailTimingAccumulator inspector = mb.getStats(topic);
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
    int port = PortNumberUtil.getFreePortNumber(7902);
    testEnableRetries(true, port);
    testEnableRetries(false, port);
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
      PintailTimingAccumulator inspector = mb.getStats(topic);
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
      int port = PortNumberUtil.getFreePortNumber(7903);
      tserver = new NtMultiServer(new ScribeAlwaysTryAgain(), port);
      tserver.start();

      int timeoutSeconds = 10;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, true, 100, 100, 10);

      String topic = "retry";
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.close();
      PintailTimingAccumulator inspector = mb.getStats(topic);
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

  // Disabling this as it fails intermittently
  //@Test
  public void testResendOnAckLost() throws Exception {
    testResendOnAckLost(true);
    testResendOnAckLost(false);
  }
  
  public void testResendOnAckLost(boolean resendOnAckLost) throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7904);
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
      PintailTimingAccumulator inspector = mb.getStats(topic);
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
