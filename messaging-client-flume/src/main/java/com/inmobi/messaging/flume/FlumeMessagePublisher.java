package com.inmobi.messaging.flume;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.AbstractMessagePublisher;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class FlumeMessagePublisher extends AbstractMessagePublisher {

  private static final Logger LOG = LoggerFactory
      .getLogger(FlumeMessagePublisher.class);

  private static final int BUFFER_SIZE = 10000;
  //private static final int CONCURRENT_SENDERS = 3;
  private RpcClient rpcClient;
  private BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>(
      BUFFER_SIZE);
  private volatile boolean stopped;
  private int batchSize;
  private Thread senderThread;

  @Override
  public void init(ClientConfig config) {
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
    Event event = EventBuilder.withBody(m.getMessage(), headers);
    synchronized (queue) {
      if (!queue.offer(event)) {
        // queue is full
        // dropping the message
        LOG.warn("Queue is full. dropping the message");
        getStats().accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);
      }
      queue.notify();
    }
  }

  @Override
  public void close() {
    super.close();
    stopped = true;
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
            if (queue.size() >= rpcClient.getBatchSize()) {
              for (int i = 0; i < rpcClient.getBatchSize(); i++) {
                batch.add(queue.remove());
              }
            }
          }
          if (batch.size() > 0) {
            try {
              LOG.info("rpcclient is Active: " + rpcClient.isActive());
              rpcClient.appendBatch(batch);
              for (int i = 0; i < batch.size(); i++) {
                getStats().accumulateOutcomeWithDelta(Outcome.SUCCESS, 0);
              }
            } catch (Exception e) {
              // TODO handle this
              for (int i = 0; i < batch.size(); i++) {
                getStats().accumulateOutcomeWithDelta(
                    Outcome.UNHANDLED_FAILURE, 0);
              }
              LOG.warn("Could not send batch of size " + batch.size(), e);
            }
          }
        }
      }
    }

  }

}
