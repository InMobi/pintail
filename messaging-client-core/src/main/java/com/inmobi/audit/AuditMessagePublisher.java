package com.inmobi.audit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class AuditMessagePublisher {

  private static final String AUDIT_CONF_FILE = "audit-conf.properties";
  private static final String WINDOW_SIZE_KEY = "window.size";
  private static final String THREAD_COUNT_KEY = "thread.count";
  private static final String AGGREGATE_WINDOW_KEY = "aggregate.window";
  private static int windowSizeInMins;
  private static int aggregateWindowSizeInMins;
  public static ConcurrentHashMap<String, AuditCounterAccumulator> topicAccumulatorMap = new ConcurrentHashMap<String, AuditCounterAccumulator>();
  private static final String tier = "publisher";
  private static ScheduledThreadPoolExecutor executor;
  private static boolean isInit = false;
  private static AuditMessageWorker worker;

  private static final Logger LOG = LoggerFactory
      .getLogger(AuditMessagePublisher.class);

  public static void init() {
    if (isInit)
      return;
    ClientConfig config = ClientConfig.loadFromClasspath(AUDIT_CONF_FILE);
    init(config);
  }

  public static void init(String confFile) {
    if (isInit)
      return;
    ClientConfig config = ClientConfig.loadFromClasspath(confFile);
    init(config);
  }

  private static void init(ClientConfig config) {
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
    worker = new AuditMessageWorker(hostname, tier, windowSizeInMins);
    executor.scheduleWithFixedDelay(worker, aggregateWindowSizeInMins,
        aggregateWindowSizeInMins, TimeUnit.MINUTES);
    // setting init flag to true
    isInit = true;
  }

  public static AuditCounterAccumulator getAccumulator(String topic) {
    if (!topicAccumulatorMap.contains(topic))
      topicAccumulatorMap.putIfAbsent(topic, new AuditCounterAccumulator(
          windowSizeInMins));
    return topicAccumulatorMap.get(topic);
  }

  public static void close() {
    if (worker != null)
      worker.run(); // flushing the last audit packet during shutdown
    executor.shutdown();
  }

  public static void attachHeaders(Message m) {

  }

}
