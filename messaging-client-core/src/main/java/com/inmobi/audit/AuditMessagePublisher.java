package com.inmobi.audit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.messaging.ClientConfig;

public class AuditMessagePublisher {

  private static final String AUDIT_CONF_FILE = "audit-conf.properties";
  private static final String WINDOW_SIZE_KEY = "window.size";
  private static final String THREAD_COUNT_KEY = "thread.count";
  private static final String AGGREGATE_WINDOW_KEY = "aggregate.window";
  private static int windowSizeInMins;
  private static int aggregateWindowSizeInMins;
  public static ConcurrentHashMap<String, AuditCounterAccumulator> topicAccumulatorMap = new ConcurrentHashMap<String, AuditCounterAccumulator>();
  private static final String tier = "publisher";
  private ScheduledThreadPoolExecutor executor;

  private static final Logger LOG = LoggerFactory
      .getLogger(AuditMessagePublisher.class);

  public void init() {
    ClientConfig config = ClientConfig.loadFromClasspath(AUDIT_CONF_FILE);
    init(config);
  }

  public void init(String confFile) {
    ClientConfig config = ClientConfig.loadFromClasspath(confFile);
    init(config);
  }

  private void init(ClientConfig config) {
    windowSizeInMins = config.getInteger(WINDOW_SIZE_KEY, 5);
    aggregateWindowSizeInMins = config.getInteger(AGGREGATE_WINDOW_KEY, 5);
    executor = new ScheduledThreadPoolExecutor(
        config.getInteger(THREAD_COUNT_KEY));
    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to find the hostanme of the local box,audit packets wont contain hostname");
      hostname = "";
    }
    executor.scheduleWithFixedDelay(new AuditMessageWorker(hostname, tier,
        windowSizeInMins), aggregateWindowSizeInMins,
        aggregateWindowSizeInMins, TimeUnit.MINUTES);

  }

  public static AuditCounterAccumulator getAccumulator(String topic) {
    if (!topicAccumulatorMap.contains(topic))
      topicAccumulatorMap.putIfAbsent(topic, new AuditCounterAccumulator(
          windowSizeInMins));
    return topicAccumulatorMap.get(topic);
  }

  public static void attachHeaders(ByteBuffer bytes) {

  }

}
