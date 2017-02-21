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

import java.util.concurrent.CountDownLatch;

import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlwaysSuccess;

import com.inmobi.messaging.netty.ScribeMessagePublisher;

public class TestMultiplePublisherThreads {
  @Test
  public void multipleThreadSinglePublisher() throws Exception{
    NtMultiServer tserver = null;
    try {
      int port = PortNumberUtil.getFreePortNumber(7932);
      tserver = new NtMultiServer(new ScribeAlwaysSuccess(), port);
      tserver.start();

      ScribeMessagePublisher publisher = TestServerStarter.createPublisher(port, 5);
      String topic = "test";
      CountDownLatch startLatch = new CountDownLatch(10);
      PublishThread p1 = new PublishThread(startLatch, topic, publisher);
      PublishThread p2 = new PublishThread(startLatch, topic, publisher);
      PublishThread p3 = new PublishThread(startLatch, topic, publisher);
      PublishThread p4 = new PublishThread(startLatch, topic, publisher);
      p1.start();
      p2.start();
      p3.start();
      p4.start();
      Thread.sleep(3000);
      p1.join();
      p2.join();
      p3.join();
      p4.join();
      System.out.println(publisher.getStats(topic));
      assertEquals(publisher.getStats(topic).getSuccessCount(), 4);
      assertEquals(publisher.getStats(topic).getRetryCount(),0);
      assertEquals(publisher.getStats(topic).getReconnectionCount(),0);
      publisher.close();
    } finally {
      tserver.stop();
    }
    System.out.println("Test multipleThreadSinglePublisher is done");    
  }

  private void doTest(String topic, ScribeMessagePublisher publisher) {
    try {
      publisher.publish(topic, new Message("msg1".getBytes()));
    } catch (PintailException e) {
      e.printStackTrace();
    }
    while (publisher.getStats(topic).getInFlight() != 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  class PublishThread extends Thread {

    private String topic;
    private ScribeMessagePublisher publisher;
    private final CountDownLatch startLatch;

    PublishThread(CountDownLatch startLatch, String topic, ScribeMessagePublisher publisher){
      this.startLatch= startLatch;
      this.topic = topic;
      this.publisher = publisher;
    }

    public void run(){
      doTest(topic, publisher);
    }
  }
}
