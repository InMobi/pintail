package com.inmobi.messaging.flume;

/*
 * #%L
 * messaging-client-flume
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;

public class FlumeMessagePublisher extends AbstractMessagePublisher {

  private static final Log LOG = LogFactory.getLog(FlumeMessagePublisher.class);

  private static final int BUFFER_SIZE = 10000;
  //private static final int CONCURRENT_SENDERS = 3;
  private RpcClient rpcClient;
  private BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>(
      BUFFER_SIZE);
  private volatile boolean stopped;
  private int batchSize;
  private Thread senderThread;

  @Override
  public void init(ClientConfig config) throws IOException {
    super.init(config);
    batchSize = config.getInteger("flume.batchsize", 1);
    /*threadPool = new ThreadPoolExecutor(CONCURRENT_SENDERS, CONCURRENT_SENDERS,
        1, TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());*/
    rpcClient = createRpcClient(config);
    LOG.info("Rpcclient is Active: " + rpcClient.isActive());
    senderThread = new Thread(new AsyncSender());
    senderThread.start();
  }

  protected RpcClient createRpcClient(ClientConfig config) {
    return RpcClientFactory.getDefaultInstance(
        config.getString("flume.host", "localhost"),
        config.getInteger("flume.port", 1111), batchSize);
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    // headers.put("streamName", "rr");
    String topic = headers.get(HEADER_TOPIC);
    Event event = EventBuilder.withBody(m.getData().array(), headers);
    synchronized (queue) {
      if (!queue.offer(event)) {
        // queue is full
        // dropping the message
        LOG.warn("Queue is full. dropping the message");
        getStats(topic).accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);
      } else {
        queue.notify();
      }
    }
  }

  @Override
  public void close() {
    super.close();
    stopped = true;
    senderThread.interrupt();
    try {
      senderThread.join();
    } catch (InterruptedException e) {
      // TODO: handle this
      e.printStackTrace();
    }
    rpcClient.close();
  }

  private class AsyncSender implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.interrupted()) {
        synchronized (queue) {
          if (queue.size() == 0) {
            try {
              queue.wait();
            } catch (InterruptedException e) {
              return;
            }
          }
          final List<Event> batch = new ArrayList<Event>();
          synchronized (queue) {
            if (queue.size() >= batchSize) {
              for (int i = 0; i < batchSize; i++) {
                batch.add(queue.remove());
              }
            }
          }
          if (batch.size() > 0) {
            try {
              LOG.info("rpcclient is Active: " + rpcClient.isActive());
              rpcClient.appendBatch(batch);
              for (int i = 0; i < batch.size(); i++) {
                getStats(batch.get(i).getHeaders().get(HEADER_TOPIC)).
                  accumulateOutcomeWithDelta(Outcome.SUCCESS, 0);
              }
            } catch (Exception e) {
              // TODO handle this
              for (int i = 0; i < batch.size(); i++) {
                getStats(batch.get(i).getHeaders().get(HEADER_TOPIC)).
                  accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);
              }
              LOG.warn("Could not send batch of size " + batch.size(), e);
            }
          }
        }
      }
    }

  }

}
