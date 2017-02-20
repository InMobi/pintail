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

import com.inmobi.messaging.publisher.PintailException;
import static org.testng.Assert.assertEquals;

import org.testng.Assert;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlwaysSuccess;
import random.pkg.ScribeAlwaysTryAgain;
import random.pkg.ScribeSlacker;

import com.inmobi.messaging.instrumentation.PintailTimingAccumulator;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestBlocked {
  @Test()
  public void testMsgQueueSize() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7945);
      tserver = new NtMultiServer(new ScribeAlwaysSuccess(), port);

      int timeoutSeconds = 2;
      // create publisher with msg queue size 1
      final ScribeMessagePublisher mb =
          TestServerStarter.createPublisher(port, timeoutSeconds, 1, true,
              true, 1, 10, true);

      final String topic = "retry";
      int numMsgs = 2;
      // publish ${numMsgs} messages
      for (int i = 0; i < numMsgs; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              mb.publish(topic, new Message("mmmm".getBytes()));
            } catch (PintailException e) {
              e.printStackTrace();
            }
          }
        }).start();
      }
      Thread.sleep(10);

      PintailTimingAccumulator inspector = mb.getStats(topic);
      assertEquals(inspector.getLostCount(), 0, "Lost incremented");
      tserver.start();
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }
      mb.close();
      System.out.println("TestLost.testMsgQueueSize :stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getLostCount(), 0, "Lost incremented");
      assertEquals(inspector.getSuccessCount(), numMsgs,
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
      final ScribeMessagePublisher mb =
          TestServerStarter.createPublisher(port, timeoutSeconds, 1, true,
              true, 1, 1, true);

      final String topic = "retry";
      int numMsgs = 3;
      // publish ${numMsgs} messages
      for (int i = 0; i < numMsgs; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              mb.publish(topic, new Message("mmmm".getBytes()));
            } catch (PintailException e) {
              e.printStackTrace();
            }
          }
        }).start();
      }
      Thread.sleep(10);
      PintailTimingAccumulator inspector = mb.getStats(topic);
      assertEquals(inspector.getLostCount(), 0, "Lost incremented");
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }
      mb.close();
      System.out.println("testAckQueueSize stats:" + inspector.toString());
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getLostCount(), 0, "Lost incremented");
      assertEquals(inspector.getSuccessCount(), numMsgs,
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
      int queueSize = 1;
      // create publisher with msgqueue size 1
      final ScribeMessagePublisher mb =
          TestServerStarter.createPublisher(port, timeoutSeconds, 1, true,
              false, queueSize, 1, 10, true);

      final String topic = "retry";

      int numMsgs = 1;
      // publish ${numMsgs} messages
      for (int i = 0; i < numMsgs; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              mb.publish(topic, new Message("mmmm".getBytes()));
            } catch (PintailException e) {
              e.printStackTrace();
            }
          }
        }).start();
      }
      Thread.sleep(10);

      PintailTimingAccumulator inspector = mb.getStats(topic);
      Assert.assertTrue(inspector.getLostCount() <= 0, "Wrong lost count");
      mb.close();
      System.out.println("testMsgQueueSizeOnRetries stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getLostCount() + inspector.getGracefulTerminates(),
          1, "Lost or Graceful terminates incremented");
      assertEquals(inspector.getSuccessCount(), 0, "success not incremented");
    } finally {
      tserver.stop();
    }
    System.out.println("TestLost.testMsgQueueSizeOnRetries done");
  }
}
