package com.inmobi.messaging.netty;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;

public class ScribeMessagePublisher extends AbstractMessagePublisher {
  private static final Log LOG = LogFactory.getLog(ScribeMessagePublisher.class);

  private static final Timer timer = new HashedWheelTimer();

  public static final String maxConnectionRetriesConfig =
      "scribe.maxconnectionretries";
  public static final int DEFAULT_MAX_CONNECTION_RETRIES = 3;

  private String host;
  private int port;
  private ClientBootstrap bootstrap;
  private volatile Channel ch = null;

  /**
   * This is meant to be a way for async callbacks to set the channel on a
   * successful connection
   * 
   * Java does not have pointers to pointers. So have to resort to sending in a
   * wrapper object that knows to update our pointer
   */
  class ChannelSetter {
    final int maxConnectionRetries;
    ChannelSetter(int maxConns) {
      maxConnectionRetries = maxConns;
    }
    public void setChannel(Channel ch) {
      Channel oldChannel = ScribeMessagePublisher.this.ch;
      if (ch != oldChannel) {
        if (oldChannel != null && oldChannel.isOpen()) {
          LOG.info("Closing old channel " + oldChannel.getId());
          oldChannel.close();
        }
        LOG.info("setting channel to " + ch.getId());
        ScribeMessagePublisher.this.ch = ch;
      }
    }

    public Channel connect() throws Exception {
      int numRetries = 0;
      Channel channel = null;
      while (true) {
        try {
          LOG.info("Connecting to scribe host:" + host + " port:" + port);
          ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
              port));
          channel =
              future.awaitUninterruptibly().getChannel();
          LOG.info("Connected to Scribe");
          setChannel(channel);
          if (!future.isSuccess()) {
            bootstrap.releaseExternalResources();
            throw new Exception(future.getCause());
          } else {
            return channel;
          }
        } catch (Exception e) {
          numRetries++;
          if (numRetries >= maxConnectionRetries) {
            throw e;
          }
          LOG.warn("Got exception while connecting. Retrying", e);
        }
      }
    }
  }

  public void init(String host, int port, int backoffSeconds,
      int timeoutSeconds, int maxConnectionRetries) throws Exception {
    this.host = host;
    this.port = port;
    bootstrap = new ClientBootstrap(NettyEventCore.getInstance().getFactory());

    ChannelSetter chs = new ChannelSetter(maxConnectionRetries);
    ScribeHandler handler = new ScribeHandler(getStats(), chs,
        backoffSeconds, timer);
    ChannelPipelineFactory cfactory = new ScribePipelineFactory(handler,
        timeoutSeconds, timer);
    bootstrap.setPipelineFactory(cfactory);
    chs.connect();
    handler.setInited();
  }

  @Override
  public void init(ClientConfig config) {
    super.init(config);
    String host = config.getString("scribe.host", "localhost");
    int port = config.getInteger("scribe.port", 1111);
    int backoffSeconds = config.getInteger("scribe.backoffSeconds", 5);
    int timeoutSeconds = config.getInteger("scribe.timeoutSeconds", 5);
    int maxConnectionRetries = config.getInteger(maxConnectionRetriesConfig,
        DEFAULT_MAX_CONNECTION_RETRIES);
    LOG.info("Initialized ScribeMessagePublisher with host:" + host + " port:" +
        " backoffSeconds:" + backoffSeconds + " timeoutSeconds:"
        + timeoutSeconds + " maxConnectionRetries:" + maxConnectionRetries);
    try {
      init(host, port, backoffSeconds, timeoutSeconds, maxConnectionRetries);
    } catch (Exception e) {
      throw new RuntimeException("Could not initialize", e);
    }
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    if (ch != null) {
      ScribeBites.publish(ch, headers.get(HEADER_TOPIC), m);
    } else {
      suggestReconnect();
    }
  }

  private void suggestReconnect() {
    LOG.warn("Suggesting reconnect as channel is null");
    getStats().accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);
    // TODO: logic for triggering reconnect
  }

  @Override
  public void close() {
    super.close();
    if (ch != null) {
      ch.close().awaitUninterruptibly();
    }
    NettyEventCore.getInstance().releaseFactory();
  }
}