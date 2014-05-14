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
import random.pkg.ScribeSlacker;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestServerDown {

  @Test()
  public void testServerDownAtStart() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7917);
      tserver = new NtMultiServer(new ScribeAlwaysSuccess(), port);

      int timeoutSeconds = 6000;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1);

      String topic = "retry";
      // publish a message and stop the server
      mb.publish(topic, new Message("mmmm".getBytes()));
      tserver.start();
      TimingAccumulator inspector = mb.getStats(topic);
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }

      // publish another message 
      mb.publish(topic, new Message("mmmm".getBytes()));

      mb.close();
      System.out.println("testServerDownAtStart stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getSuccessCount(), 2,
          "success not incremented");
      Assert.assertTrue(inspector.getUnhandledExceptionCount() > 0,
          "Exception count not incremented");
      assertEquals(inspector.getReconnectionCount(), 1,
          "Exception count not incremented");
    } finally {
      tserver.stop();
    }
    System.out.println("TestServerDown.testServerDownAtStart done");
  }

  @Test()
  public void testServerDownAckLost() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7918);
      tserver = new NtMultiServer(new ScribeSlacker(), port);
      tserver.start();

      int timeoutSeconds = 6000;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, false, 100, 100, 0);

      String topic = "retry";
      //  publish the message and stop the server
      mb.publish(topic, new Message("mmmm".getBytes()));
      tserver.stop();
      TimingAccumulator inspector = mb.getStats(topic);
      mb.close();
      System.out.println("testServerDownAckLost stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getSuccessCount(), 0,
          "success incremented");
      assertEquals(inspector.getGracefulTerminates(), 1,
          "ack not lost");
    } finally {
      tserver.stop();
    }
    System.out.println("TestServerDown.testServerDownAckLost done");
  }

  @Test()
  public void testServerGoingDownInTheMiddle() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7916);
      tserver = new NtMultiServer(new ScribeAlwaysSuccess(), port);
      tserver.start();

      int timeoutSeconds = 6000;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1);

      String topic = "retry";
      // publish a message and stop the server
      mb.publish(topic, new Message("mmmm".getBytes()));
      TimingAccumulator inspector = mb.getStats(topic);
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }

      tserver.stop();

      Thread.sleep(1000);
      // publish another message and start the server
      mb.publish(topic, new Message("mmmm".getBytes()));
      tserver.start();

      System.out.println("testServerGoingDownInTheMiddle stats:" + 
          inspector.toString());
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }

      // publish another message
      mb.publish(topic, new Message("mmmm".getBytes()));
      while (inspector.getInFlight() != 0) {
        Thread.sleep(10);
      }
      mb.close();
      System.out.println("testServerGoingDownInTheMiddle stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getSuccessCount(), 3,
          "success not incremented");
      /* Instead of below two asserts it should be that success count should
      // get incremented for all messages. But When NtMultiServer is stopped,
      // it is not resulting
      // in any of the channel disconnect or close immediately until we send a
      // message. So, the message published is sent and then channel disconnects
       which results in ack lost. */
      /*
      assertEquals(inspector.getSuccessCount(), 2,
          "success not incremented");
      assertEquals(inspector.getGracefulTerminates(), 1,
          "success not incremented");
       */
      assertEquals(inspector.getReconnectionCount(),  1,
          "Exception count not incremented");
    } finally {
      tserver.stop();
    }
    System.out.println("TestServerDown.testServerGoingDownInTheMiddle done");
  }

  @Test()
  public void testServerDownMsgLost() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7919);
      tserver = new NtMultiServer(new ScribeAlwaysSuccess(), port);

      int timeoutSeconds = 6000;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1, true, true, 100, 100, 10);

      String topic = "retry";
      // send a message without starting the server and close the publisher
      mb.publish(topic, new Message("mmmm".getBytes()));
      mb.close();
      TimingAccumulator inspector = mb.getStats(topic);
      System.out.println("testServerDownMsgLost stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getSuccessCount(), 0,
          "success not incremented");
      assertEquals(inspector.getLostCount(), 1,
          "lost not incremented");
      Assert.assertTrue(inspector.getUnhandledExceptionCount() > 0,
          "Exception count not incremented");
    } finally {
      tserver.stop();
    }
    System.out.println("TestServerDown.testServerDownMsgLost done");
  }

}
