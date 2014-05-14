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

import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlwaysSuccess;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestSimple {
  private NtMultiServer server;
  private ScribeMessagePublisher publisher;
  private int port = PortNumberUtil.getFreePortNumber(7922);

  @Test()
  public void simpleSend() throws Exception {
    server = new NtMultiServer(new ScribeAlwaysSuccess(), port);
    server.start();
    try {
      runTest();
      //create the publisher again
      runTest();
    } finally {
      server.stop();
      if (publisher != null)
        publisher.close();
    }
    System.out.println("TestSimple.simpleSend() done");
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
    publisher = TestServerStarter.createPublisher(port, 5);
    sendMessages();
    publisher.close();
  }
}
