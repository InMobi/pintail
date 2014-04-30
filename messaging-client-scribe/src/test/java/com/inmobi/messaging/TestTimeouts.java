package com.inmobi.messaging;

/*
 * #%L
 * messaging-client-scribe
 * %%
 * Copyright (C) 2014 InMobi
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

public class TestTimeouts {

  /**
   * Sends a message and keeps idle for sometime so that ReadTimeOutexception is
   * thrown.
   * Sends another message.
   * 
   * @throws Exception
   */
  @Test()
  public void simpleSend() throws Exception {
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7914);
      tserver = new NtMultiServer(new ScribeAlwaysSuccess(), port);
      tserver.start();

      int timeoutSeconds = 2;
      ScribeMessagePublisher mb = TestServerStarter.createPublisher(port,
          timeoutSeconds, 1);
      String topic = "ch";
      mb.publish(topic, new Message("mmmm".getBytes()));
      TimingAccumulator inspector = mb.getStats(topic);

      Thread.sleep((timeoutSeconds + 1) * 1000);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getSuccessCount(), 1,
          "not sent succeessfully");
      assertEquals(inspector.getUnhandledExceptionCount(), 1,
          "check if recorded as error");

      // publish another message
      mb.publish(topic, new Message("mmmm".getBytes()));
      Thread.sleep((timeoutSeconds + 1) * 1000);
      mb.close();
      System.out.println("TestTimeouts.simpleSend stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getSuccessCount(), 2,
          "not sent succeessfully");
      assertEquals(inspector.getUnhandledExceptionCount(), 2,
          "check if recorded as error");
      Assert.assertTrue(inspector.getReconnectionCount() > 0,
          "not reconnected on timeout");
    } finally {
      tserver.stop();
    }
    System.out.println("TestTimeouts.simpleSend done");
  }

  @Test()
  public void testSlackingServer() throws Exception {
    NtMultiServer tserver = null;
    ScribeMessagePublisher mb = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7924);
      tserver = new NtMultiServer(new ScribeSlacker(), port);
      tserver.start();

      int timeoutSeconds = 1;
      mb = TestServerStarter.createPublisher(port, timeoutSeconds);

      String topic = "ch";
      mb.publish(topic, new Message("mmmm".getBytes()));
      TimingAccumulator inspector = mb.getStats(topic);

      Thread.sleep((timeoutSeconds + 1) * 1000);
      mb.close();
      System.out.println("testSlackingServer.stats:" + inspector);
      assertEquals(inspector.getInFlight(), 0,
          "ensure not considered midflight");
      assertEquals(inspector.getSuccessCount(), 1,
          "not sent succeessfully");
      Assert.assertTrue(inspector.getUnhandledExceptionCount() > 0,
          "check if recorded as error");
      assertEquals(inspector.getReconnectionCount(), 0,
          "reconnected on timeout for slacking server");
    } finally {
      tserver.stop();
    }
    System.out.println("TestTimeouts.testSlackingServer done");
  }
}
