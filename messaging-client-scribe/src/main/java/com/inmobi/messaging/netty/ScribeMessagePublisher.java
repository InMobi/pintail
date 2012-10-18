package com.inmobi.messaging.netty;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;

public class ScribeMessagePublisher extends AbstractMessagePublisher implements
    ScribePublisherConfiguration{
  public static final Log LOG = LogFactory.getLog(ScribeMessagePublisher.class);

  private String host;
  private int port;
  private int backoffSeconds;
  private int timeoutSeconds;
  private int maxConnectionRetries;

  private Map<String, ScribeTopicPublisher> scribeConnections = new
      HashMap<String, ScribeTopicPublisher>();
  @Override
  public void init(ClientConfig config) throws IOException {
    super.init(config);
    init(config.getString("scribe.host", "localhost"),
        config.getInteger("scribe.port", 1111),
        config.getInteger("scribe.backoffSeconds", 5),
        config.getInteger("scribe.timeoutSeconds", 5),
        config.getInteger(maxConnectionRetriesConfig,
            DEFAULT_MAX_CONNECTION_RETRIES));
  }

  public void init(String host, int port, int backoffSeconds, int timeout,
      int maxConnectionRetries) {
    this.host = host;
    this.port = port;
    this.backoffSeconds = backoffSeconds;
    this.timeoutSeconds = timeout;
    this.maxConnectionRetries = maxConnectionRetries;
    LOG.info("Initialized ScribeMessagePublisher with host:" + host + " port:" +
        + port + " backoffSeconds:" + backoffSeconds + " timeoutSeconds:"
        + timeoutSeconds + " maxConnectionRetries:" + maxConnectionRetries);
  }

  protected void initTopic(String topic, TimingAccumulator stats) {
    super.initTopic(topic, stats);
    if (scribeConnections.get(topic) == null) {
      ScribeTopicPublisher connection = new ScribeTopicPublisher();
      scribeConnections.put(topic, connection);
      connection.init(topic, host, port, backoffSeconds, timeoutSeconds,
          maxConnectionRetries, stats);
    }
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    String topic = headers.get(HEADER_TOPIC);
    scribeConnections.get(topic).publish(m);
  }

  public void close() {
    for (ScribeTopicPublisher connection : scribeConnections.values()) {
      connection.close();
    }
    super.close();
  }
}
