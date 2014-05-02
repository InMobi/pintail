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

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import scribe.thrift.ResultCode;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.Message;

public class ScribeTopicPublisher {
  private static final Log LOG = LogFactory.getLog(ScribeTopicPublisher.class);

  private final Timer timer = new HashedWheelTimer();

  private ClientBootstrap bootstrap;
  private volatile Channel thisChannel = null;
  private String topic;
  private String host;
  private int port;
  private TimingAccumulator stats;
  private BlockingQueue<Message> toBeSent;
  private BlockingQueue<Message> toBeAcked;
  private long sleepInterval = 10;
  private boolean stopped = false;
  private Thread senderThread;
  private ScribeHandler handler;
  private boolean resendOnAckLost = false;
  private boolean reconnectionInProgress = false;
  private boolean enabledRetries = true;
  private int numDrainsOnClose = 10;
  // Reentrant lock used to synchronize sending messages from send queue.
  private final ReentrantLock sendLock = new ReentrantLock();

  /**
   * This is meant to be a way for async callbacks to set the channel on a
   * successful connection
   *
   * Java does not have pointers to pointers. So have to resort to sending in a
   * wrapper object that knows to update our pointer
   */
  class ChannelSetter {
    public Channel getCurrentChannel() {
      return ScribeTopicPublisher.this.thisChannel;
    }

    public void setChannel(Channel ch) {
      Channel oldChannel = ScribeTopicPublisher.this.thisChannel;
      if (ch != oldChannel) {
        LOG.info("setting channel to " + ch.getId());
        ScribeTopicPublisher.this.thisChannel = ch;
        if (oldChannel != null && oldChannel.isOpen()) {
          LOG.info("Closing old channel " + oldChannel.getId());
          oldChannel.close().awaitUninterruptibly();
        }
      }
    }

    public Channel connect() throws Exception {
      Channel channel = null;
      try {
        LOG.info("Connecting to scribe host:" + host
            + " port:" + port);
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
            port));
        channel =
            future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
          LOG.info("Could not connect to Scribe. Error:", future.getCause());
          if (future.getCause() instanceof Exception) {
            throw (Exception) future.getCause();
          } else {
            throw new RuntimeException(future.getCause());
          }
        } else {
          LOG.info("Connected to Scribe");
          setChannel(channel);
          return channel;
        }
      } catch (Exception e) {
        throw e;
      }
    }
  }

  public void init(String topic, String host, int port, int backoffSeconds,
      int timeoutSeconds, TimingAccumulator stats, boolean enableRetries,
      boolean resendOnAckLost, long sleepInterval, int msgQueueSize,
      int ackQueueSize, int numDrainsOnClose) {
    this.topic = topic;
    this.stats = stats;
    this.host = host;
    this.port = port;
    this.enabledRetries = enableRetries;
    this.resendOnAckLost = resendOnAckLost;
    this.sleepInterval = sleepInterval;

    this.toBeSent = new LinkedBlockingQueue<Message>(msgQueueSize);
    // create ack queue only if retry is enabled
    if (enableRetries) {
      this.toBeAcked = new LinkedBlockingQueue<Message>(ackQueueSize);
    }
    this.numDrainsOnClose = numDrainsOnClose;

    bootstrap = new ClientBootstrap(NettyEventCore.getInstance().getFactory());

    ChannelSetter chs = new ChannelSetter();
    handler = new ScribeHandler(stats, chs, backoffSeconds, timer, this);
    ChannelPipelineFactory cfactory = new ScribePipelineFactory(handler,
        timeoutSeconds, timer);
    bootstrap.setPipelineFactory(cfactory);
    try {
      chs.connect();
    } catch (Exception ioe) {
      LOG.info("Could not intialize the connection, scheduling reconnect");
      handler.setInited();
      handler.scheduleReconnect();
    }
    handler.setInited();
    senderThread = new Thread(new AsyncSender());
    senderThread.start();
  }

  protected void publish(Message m) {
    addToSend(m);
    trySending(true);
  }

  private boolean addToSend(Message m) {
    if (!toBeSent.offer(m)) {
      LOG.warn("Messages to be sent Queue is full,"
          + " dropping the message");
      stats.accumulateOutcomeWithDelta(Outcome.LOST, 0);
      return false;
    }
    return true;
  }

  boolean isSendQueueEmpty() {
    return toBeSent.size() == 0;
  }

  boolean isAckQueueEmpty() {
    return (!enabledRetries || toBeAcked.size() == 0);
  }

  void trySending(boolean tryLock) {
    if (isSendQueueEmpty()) {
      return;
    }
    if (isChannelConnected()) {
      if (isChannelWritable()) {
        // if tryLock is true, then acquire tryLock else acquire lock
        if (!tryLock) {
          sendLock.lock();
        } else {
          // if tryLock fails, then return. The assumption is that the thread
          // that has already acquired lock will try to send all messages.
          if (!sendLock.tryLock()) {
            return;
          }
        }

        try {
          Message m = null;
          while ((m = toBeSent.peek()) != null) {
            // Add this message to ack queue before writing the message.
            // Also add a clone of this message to ack queue.
            if (enabledRetries && (toBeAcked.remainingCapacity() == 0
                || !toBeAcked.offer(m.clone()))) {
              LOG.info("Could not send earlier messages successfully, not"
                  + " sending right now.");
              break;
            }
            // write the current message
            ScribeBites.publish(thisChannel, topic, m);
            // remove the message from sent queue
            toBeSent.poll();
            // check if the next message can be written immediately
            if (!isChannelWritable()) {
              break;
            }
          }
        } finally {
          sendLock.unlock();
        }
      }
    } else {
      suggestReconnect();
    }
  }

  private class AsyncSender implements Runnable {
    @Override
    public void run() {
      while (!stopped && !Thread.interrupted()) {
        trySending(false);
        try {
          Thread.sleep(sleepInterval);
        } catch (InterruptedException ie) {
          LOG.info("Async sender interrupted. Exiting");
        }
      }
    }
  }

  private boolean isChannelConnected() {
    if (thisChannel == null) {
      LOG.info("Channel is not initialized yet, not sending right now");
      return false;
    }
    if (!thisChannel.isConnected()) {
      LOG.info("Channel is not connected, not sending right now");
      return false;
    }
    return true;
  }

  private boolean isChannelWritable() {
    if (reconnectionInProgress) {
      LOG.info("Reconnection in progress, not sending right now");
      return false;
    }
    if (!thisChannel.isWritable()) {
      LOG.info("Channel is not writable, not sending right now");
      return false;
    }
    return true;
  }

  void suggestReconnect() {
    handler.scheduleReconnect();
  }

  private void drainAll()  {
    LOG.info("Draining all the messages");
    int numRetries = 0;
    while (true) {
      trySending(false);
      if (isSendQueueEmpty() && isAckQueueEmpty()) {
        break;
      }
      if ((numDrainsOnClose != -1 && numRetries > numDrainsOnClose)) {
        LOG.info("Dropping messages as channel is not connected or number of"
            + " retries exhausted");
        emptyAckQueue();
        emptyMsgQueue();
      }
      numRetries++;
      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        LOG.info("Draining inturrupted. Exiting");
        return;
      }
    }
  }

  void prepareReconnect() {
    reconnectionInProgress = true;
    emptyAckQueue();
  }

  void doneReconnect() {
    reconnectionInProgress = false;
  }

  void emptyAckQueue() {
    if (!enabledRetries) {
      return;
    }
    if (resendOnAckLost) {
      Message m = null;
      while ((m = toBeAcked.poll()) != null) {
        addToSend(m);
      }
    } else {
      if (toBeAcked.size() > 0) {
        LOG.warn("Emptying ack queue of size:" + toBeAcked.size());
      }
      while (toBeAcked.poll() != null) {
        stats.accumulateOutcomeWithDelta(Outcome.GRACEFUL_FAILURE, 0);
      }
    }
  }

  void emptyMsgQueue() {
    if (toBeSent.size() > 0) {
      LOG.warn("Emptying message queue of size:" + toBeSent.size());
    }
    while (toBeSent.poll() != null) {
      stats.accumulateOutcomeWithDelta(Outcome.LOST, 0);
    }
  }

  public void close() {
    stopped = true;
    if (senderThread != null) {
      senderThread.interrupt();
      try {
        senderThread.join();
      } catch (InterruptedException e) {
        LOG.info("join on sender Thread interrupted");
      }
    }
    drainAll();
    LOG.info("Closing the channel");
    handler.prepareClose();
    if (thisChannel != null) {
      thisChannel.close().awaitUninterruptibly();
    }
    timer.stop();
    NettyEventCore.getInstance().releaseFactory();
  }

  void ack(ResultCode success) {
    // first check the result code. If it is success, then increment the
    // success counter and remove the message from ack queue, if configured
    if (success.getValue() == 0) {
      if (enabledRetries) {
        toBeAcked.poll();
      }
      stats.accumulateOutcomeWithDelta(Outcome.SUCCESS, 0);
    } else {
      // else if it is try later, then remove the message from ack queue
      // and add to send queue
      if (enabledRetries) {
        LOG.info("Could not send the message successfully, resending");
        Message m = toBeAcked.poll();
        if (m != null) {
          // If the message gets added to send queue, then increment the retry
          // count. Else the lost count will get incremented if add fails.
          if (addToSend(m)) {
            stats.accumulateOutcomeWithDelta(Outcome.RETRY, 0);
          }
        } else {
          LOG.info("Could not send, as acked message not found");
        }
      } else {
        LOG.warn("Could not send the message successfully. Got TRY_LATER");
        stats.accumulateOutcomeWithDelta(Outcome.GRACEFUL_FAILURE, 0);
      }
    }
  }
}
