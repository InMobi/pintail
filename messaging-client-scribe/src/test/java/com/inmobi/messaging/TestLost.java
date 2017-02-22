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

import static org.testng.Assert.assertEquals;

import org.testng.Assert;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlwaysSuccess;
import random.pkg.ScribeAlwaysTryAgain;
import random.pkg.ScribeSlacker;

import com.inmobi.messaging.instrumentation.PintailTimingAccumulator;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestLost {
  @Test()
  public void testMsgQueueSize() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7945);
      tserver = new NtMultiServer(new ScribeAlwaysSuccess(), port);

      int timeoutSeconds = 2;
      // create publisher with msg queue size 1
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, true, 1, 10);

      String topic = "retry";
      // publish two messages
      try {
        mb.publish(topic, new Message("mmmm".getBytes()));
      } catch (PintailException e) {
        e.printStackTrace();
      }
      try {
        mb.publish(topic, new Message("mmmm".getBytes()));
      } catch (PintailException e) {
        e.printStackTrace();
      }
      PintailTimingAccumulator inspector = mb.getStats(topic);
      assertEquals(inspector.getRejectCount(), 1,
          "Reject not incremented");
      tserver.start();
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }
      mb.close();
      System.out.println("TestLost.testMsgQueueSize :stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getRejectCount(), 1,
          "Reject not incremented");
      assertEquals(inspector.getSuccessCount(), 1,
          "success not incremented");
    } finally {
      tserver.stop();
    }
    System.out.println("TestLost.testMsgQueueSize done");
  }

  @Test()
  public void testAckQueueSize() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7946);
      tserver = new NtMultiServer(new ScribeSlacker(), port);
      tserver.start();

      int timeoutSeconds = 2;
      // create publisher with msgqueue size 1
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, true, 1, 1);

      String topic = "retry";
      // publish 3 messages
      try {
        mb.publish(topic, new Message("mmmm".getBytes()));
      } catch (PintailException e) {
        e.printStackTrace();
      }
      try {
        mb.publish(topic, new Message("mmmm".getBytes()));
      } catch (PintailException e) {
        e.printStackTrace();
      }
      try {
        mb.publish(topic, new Message("mmmm".getBytes()));
      } catch (PintailException e) {
        e.printStackTrace();
      }
      PintailTimingAccumulator inspector = mb.getStats(topic);
      assertEquals(inspector.getRejectCount(), 1,
          "Reject not incremented");
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }
      mb.close();
      System.out.println("testAckQueueSize stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getRejectCount(), 1,
          "Reject not incremented");
      assertEquals(inspector.getSuccessCount(), 2,
          "success not incremented");
    } finally {
      tserver.stop();
    }
    System.out.println("TestLost.testAckQueueSize done");
  }

  @Test()
  public void testMsgQueueSizeOnRetries() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7947);
      tserver = new NtMultiServer(new ScribeAlwaysTryAgain(), port);
      tserver.start();

      int timeoutSeconds = 2;
      // create publisher with msgqueue size 1
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, true, 1, 1, 10);

      String topic = "retry";
      // publish 3 messages
      try {
        mb.publish(topic, new Message("mmmm".getBytes()));
      } catch (PintailException e) {
        e.printStackTrace();
      }
      try {
        mb.publish(topic, new Message("mmmm".getBytes()));
      } catch (PintailException e) {
        e.printStackTrace();
      }
      try {
        mb.publish(topic, new Message("mmmm".getBytes()));
      } catch (PintailException e) {
        e.printStackTrace();
      }
      PintailTimingAccumulator inspector = mb.getStats(topic);
      Assert.assertTrue(inspector.getRejectCount() == 1,
          "Wrong Reject count");
      mb.close();
      System.out.println("testMsgQueueSizeOnRetries stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getLostCount(), 2,
          "Lost not incremented");
    } finally {
      tserver.stop();
    }
    System.out.println("TestLost.testMsgQueueSizeOnRetries done");
  }
}
