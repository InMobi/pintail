package com.inmobi.messaging.netty;

import java.net.InetSocketAddress;
import java.util.Map;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.AbstractMessagePublisher;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class ScribeMessagePublisher extends AbstractMessagePublisher {
  private static final Timer timer = new HashedWheelTimer();

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
    public void setChannel(Channel ch) {
      Channel oldChannel = ScribeMessagePublisher.this.ch;
      if (oldChannel != null && oldChannel.isOpen())
        oldChannel.close();
      ScribeMessagePublisher.this.ch = ch;
    }

    public void connect() {
      bootstrap.connect(new InetSocketAddress(host, port));
    }
  }

  public void init(String host, int port, 
      int backoffSeconds, int timeoutSeconds) {
    bootstrap = new ClientBootstrap(NettyEventCore.getInstance().getFactory());

    ScribeHandler handler = new ScribeHandler(getStats(), new ChannelSetter(),
        backoffSeconds, timer);
    ChannelPipelineFactory cfactory = new ScribePipelineFactory(handler,
        timeoutSeconds, timer);

    bootstrap.setPipelineFactory(cfactory);
    bootstrap.connect(new InetSocketAddress(host, port));
  }

  @Override
  public void init(ClientConfig config) {
    super.init(config);
    String host = config.getString("host", "localhost");
    int port = config.getInteger("port", 1111);
    int backoffSeconds = config.getInteger("backoffSeconds", 5);
    int timeoutSeconds = config.getInteger("timeoutSeconds", 5);
    init(host, port, backoffSeconds, timeoutSeconds);
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    if (ch != null) {
      ScribeBites.publish(ch, m.getTopic(), m);
    } else {
      suggestReconnect();
    }
  }

  private void suggestReconnect() {
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