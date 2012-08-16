package com.inmobi.messaging.publisher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.instrumentation.MessagingClientStats;
import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

/**
 * Abstract class implementing {@link MessagePublisher} interface.
 * 
 * Initializes {@link StatsEmitter} and {@link StatsExposer} with configuration
 * defined in file {@value MessagePublisherFactory#EMITTER_CONF_FILE_KEY}. If 
 * no such file exists, statistics will be disabled.
 */
public abstract class AbstractMessagePublisher implements MessagePublisher {

  private static final Logger LOG = LoggerFactory
      .getLogger(AbstractMessagePublisher.class);
  private TimingAccumulator stats = new TimingAccumulator();
  private MessagingClientStats statsEmitter = new MessagingClientStats();
  public static final String CONTEXT_NAME = "messaging_type";
  public static final String HEADER_TOPIC = "topic";
  public static final String STATS_TYPE = "application";

  @Override
  public void publish(String topicName, Message m) {
    getStats().accumulateInvocation();
    // TODO: generate headers
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(HEADER_TOPIC, topicName);
    publish(headers, m);
  }

  protected abstract void publish(Map<String, String> headers, Message m);

  MessagingClientStats getMetrics() {
    return statsEmitter;
  }

  public TimingAccumulator getStats() {
    return stats;
  }

  protected void init(ClientConfig config) throws IOException {
    try {
      String emitterConfig = config
          .getString(MessagePublisherFactory.EMITTER_CONF_FILE_KEY);
      if (emitterConfig == null) {
        LOG.warn("Stat emitter is disabled as config "
            + MessagePublisherFactory.EMITTER_CONF_FILE_KEY + " is not set in the config.");
        return;
      }
      final Map<String, String> contexts = new HashMap<String, String>();
      contexts.put(CONTEXT_NAME, STATS_TYPE);
      statsEmitter.init(emitterConfig, stats.getHashMap(), contexts);
    } catch (Exception e) {
      throw new IOException("Couldn't find or initialize the configured stats" +
      		" emitter", e);
    }
  }

  @Override
  public void close() {
    statsEmitter.close();
  }
}
