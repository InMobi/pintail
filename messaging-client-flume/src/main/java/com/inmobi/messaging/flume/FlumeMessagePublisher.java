package com.inmobi.messaging.flume;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;

import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.AbstractMessagePublisher;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class FlumeMessagePublisher extends AbstractMessagePublisher {

  private static final int BUFFER_SIZE = 10000;
  private static final int CONCURRENT_SENDERS = 3;
  private RpcClient rpcClient;
  private BlockingQueue<Event> queue = 
      new LinkedBlockingQueue<Event>(BUFFER_SIZE);
  private volatile boolean stopped;
  protected ThreadPoolExecutor threadPool;
  private Thread senderThread;

  @Override
  public void init(String topic, ClientConfig config) {
    super.init(topic, config);
    threadPool = new ThreadPoolExecutor(CONCURRENT_SENDERS, 
        CONCURRENT_SENDERS, 1, TimeUnit.HOURS,
        new LinkedBlockingQueue<Runnable>());
    rpcClient = RpcClientFactory.getInstance(config.getString("host",
        "localhost"), config.getInteger("port", 1111));
    senderThread = new Thread(new AsyncSender());
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    Event event = new FlumeEvent(headers, m);
    synchronized(queue) {
      if (!queue.offer(event)) {
        //queue is full
        //dropping the message
      }
      queue.notify();
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
      //TODO: handle this
      e.printStackTrace();
    }
    rpcClient.close();
    threadPool.shutdown();
  }

  private class AsyncSender implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.interrupted()) {
        synchronized(queue) {
          if (queue.size() == 0) {
            try {
              queue.wait();
            } catch (InterruptedException e) {
              return;
            }
          }
          final List<Event> batch = new ArrayList<Event>();
          queue.drainTo(batch);
          threadPool.execute(new Runnable() {

            @Override
            public void run() {
              try {
                rpcClient.appendBatch(batch);
              } catch (EventDeliveryException e) {
                // TODO handle this
                getStats().accumulateOutcomeWithDelta(
                    Outcome.UNHANDLED_FAILURE, 0);
                e.printStackTrace();
              }
            }
          });
        }
      }
    }
    
  }
  private final static class FlumeEvent implements Event {

    private byte[] body;
    private Map<String, String> headers;

    FlumeEvent(Map<String, String> headers, Message m) {
      setHeaders(headers);
      setBody(m.getMessage());
    }

    @Override
    public byte[] getBody() {
      return body;
    }

    @Override
    public Map<String, String> getHeaders() {
      return headers;
    }

    @Override
    public void setBody(byte[] b) {
      body = b;
    }

    @Override
    public void setHeaders(Map<String, String> h) {
      headers = h;
    }
    
  }

}
