package com.inmobi.messaging.publisher;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.instrumentation.MessagingClientStatBuilder;
import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.AuditUtil;
import com.inmobi.stats.StatsEmitter;
import com.inmobi.stats.StatsExposer;

/**
 * Abstract class implementing {@link MessagePublisher} interface.
 *
 * Initializes {@link StatsEmitter} and {@link StatsExposer} with configuration
 * defined in file {@value MessagePublisherFactory#EMITTER_CONF_FILE_KEY}. If no
 * such file exists, statistics will be disabled.
 */
public abstract class AbstractMessagePublisher implements MessagePublisher {

  private static final Log LOG = LogFactory
      .getLog(AbstractMessagePublisher.class);
  private Map<String, TopicStatsExposer> statsExposers =
      new HashMap<String, TopicStatsExposer>();
  private MessagingClientStatBuilder statsEmitter =
      new MessagingClientStatBuilder();
  public static final String HEADER_TOPIC = "topic";
  private boolean isAuditEnabled;
  private final AuditService auditService = new AuditService(this);
  public static final String AUDIT_ENABLED_KEY = "audit.enabled";
  private volatile boolean closing = false;

  @Override
  public void publish(String topicName, Message m) {
    if (topicName == null) {
      throw new IllegalArgumentException("Cannot publish to null topic");
    }
    if (m == null) {
      throw new IllegalArgumentException("Cannot publish null message");
    }
    if (closing) {
      throw new IllegalStateException("publish cannot happen on closed "
          + "publisher");
    }
    publish(topicName, m, false);
  }

  void publish(String topicName, Message m,
      boolean isPublishedByAuditService) {
    Long timestamp = null;
    if (!isPublishedByAuditService && isAuditEnabled) {
      // Add timstamp to the message
      timestamp = new Date().getTime();
      AuditUtil.attachHeaders(m, timestamp);

    }
    // initialization should happen only by one thread
    synchronized (this) {
      if (getStats(topicName) == null) {
        TimingAccumulator stats = new TimingAccumulator();
        initTopicStats(topicName, stats);
      }
      getStats(topicName).accumulateInvocation();
      initTopic(topicName, getStats(topicName));
      if (!isPublishedByAuditService && isAuditEnabled) {
        auditService.incrementReceived(topicName, timestamp);
      }
    }
    // TODO: generate headers
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(HEADER_TOPIC, topicName);
    publish(headers, m);
  }

  protected void initTopic(String topic, TimingAccumulator stats) {
  }

  protected void closeTopic(String topic) {

  }

  /**
   * Initializes stats for the topic
   *
   * @param topic
   * @param stats
   * @throws IOException
   */
  private void initTopicStats(String topic, TimingAccumulator stats) {
    TopicStatsExposer statsExposer = new TopicStatsExposer(topic, stats);
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

  protected synchronized void init(ClientConfig config) throws IOException {
    try {
      String emitterConfig =
          config.getString(MessagePublisherFactory.EMITTER_CONF_FILE_KEY);
      isAuditEnabled = config.getBoolean(AUDIT_ENABLED_KEY, false);
      LOG.info("Audit is enabled for this publisher :" + isAuditEnabled);
      if (isAuditEnabled) {
        auditService.init(config);
      }
      if (emitterConfig == null) {
        LOG.warn("Stat emitter is disabled as config "
            + MessagePublisherFactory.EMITTER_CONF_FILE_KEY + " is not set in"
            + " the config.");
        return;
      }
      statsEmitter.init(emitterConfig);
    } catch (Exception e) {
      throw new IOException("Couldn't find or initialize the configured stats"
          + " emitter", e);
    }
  }

  @Override
  public synchronized void close() {
    closing = true;
    LOG.info("Closing the topics and stat exposers");
    for (Entry<String, TopicStatsExposer> entry : statsExposers.entrySet()) {
      String topicName = entry.getKey();
      if (topicName != AuditUtil.AUDIT_STREAM_TOPIC_NAME) {
        closeTopic(topicName);
        statsEmitter.remove(entry.getValue());
      }
    }
    if (isAuditEnabled) {
      auditService.close();
      // check whether _audit topic exist in statsexposer to ensure that some
      // messages has been published on _audit.There is a case where publisher
      // has audit enabled but since no messages were published hence no audit
      // would have been generated
      if (statsExposers.containsKey(AuditUtil.AUDIT_STREAM_TOPIC_NAME)) {
        closeTopic(AuditUtil.AUDIT_STREAM_TOPIC_NAME);
        statsEmitter.remove(statsExposers
            .get(AuditUtil.AUDIT_STREAM_TOPIC_NAME));
      }
    }
  }

  protected synchronized void init() throws IOException {
    auditService.init();

  }
}
