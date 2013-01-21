package com.inmobi.messaging.publisher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.audit.AuditCounterAccumulator;
import com.inmobi.audit.AuditMessagePublisher;
import com.inmobi.instrumentation.MessagingClientStatBuilder;
import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.stats.StatsEmitter;
import com.inmobi.stats.StatsExposer;

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
  private Map<String, TopicStatsExposer> statsExposers = new HashMap<String,
      TopicStatsExposer>();
  private MessagingClientStatBuilder statsEmitter = new 
      MessagingClientStatBuilder();
  public static final String HEADER_TOPIC = "topic";

  @Override
  public void publish(String topicName, Message m) {
    if (topicName == null) {
      throw new IllegalArgumentException("Cannot publish to null topic");
    }
    if (m == null) {
      throw new IllegalArgumentException("Cannot publish null message");
    }
    // initialization should happen only by one thread
    synchronized (this) {
      if (getStats(topicName) == null) {
        TimingAccumulator stats = new TimingAccumulator();
        initTopicStats(topicName, stats);
      }
      getStats(topicName).accumulateInvocation();
      initTopic(topicName, getStats(topicName));
    }
    // TODO: generate headers
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(HEADER_TOPIC, topicName);
    // Add timstamp to the message
    Long timestamp = System.currentTimeMillis();
    m = AuditMessagePublisher.attachHeaders(m, timestamp);
    publish(headers, m);
    incrementSent(topicName, timestamp);
  }

  private void incrementSent(String topicName, Long timestamp) {
    AuditCounterAccumulator accumulator = AuditMessagePublisher
        .getAccumulator(topicName);
    accumulator.incrementSent(timestamp);
  }

  protected void initTopic(String topic, TimingAccumulator stats) {}

  /**
   * Initializes stats for the topic
   * 
   * @param topic
   * @param stats
   * @throws IOException
   */
  private void initTopicStats(String topic, TimingAccumulator stats) {
    TopicStatsExposer statsExposer = new TopicStatsExposer(topic,
        stats);
    statsEmitter.add(statsExposer);
    statsExposers.put(topic, statsExposer);
  }

  protected abstract void publish(Map<String, String> headers, Message m);

  MessagingClientStatBuilder getMetrics() {
    return statsEmitter;
  }

  public TimingAccumulator getStats(String topic) {
    if (statsExposers.get(topic) != null) {
      return statsExposers.get(topic).getTimingAccumulator();
    } else {
      return null;
    }
  }

  TopicStatsExposer getStatsExposer(String topic) {
    return statsExposers.get(topic);
  }

  protected void init(ClientConfig config) throws IOException {
    try {
      String emitterConfig = config
          .getString(MessagePublisherFactory.EMITTER_CONF_FILE_KEY);
      String auditConfig = config
          .getString(MessagePublisherFactory.AUDIT_CONF_FILE_KEY);
      if (auditConfig == null) {
        AuditMessagePublisher.init();
      } else {
        AuditMessagePublisher.init(auditConfig);
      }
      if (emitterConfig == null) {
        LOG.warn("Stat emitter is disabled as config "
            + MessagePublisherFactory.EMITTER_CONF_FILE_KEY + " is not set in" +
            		" the config.");
        return;
      }
      statsEmitter.init(emitterConfig);
    } catch (Exception e) {
      throw new IOException("Couldn't find or initialize the configured stats" +
      		" emitter", e);
    }
  }

  @Override
  public void close() {
    LOG.info("Closing the stat exposers");
    for (StatsExposer statsExposer : statsExposers.values()) {
      statsEmitter.remove(statsExposer);
    }
    AuditMessagePublisher.close();
  }

  protected void init() {
    AuditMessagePublisher.init();

  }
}
