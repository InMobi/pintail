package com.inmobi.messaging.publisher;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.AuditUtil;

/*
 * This class is not thread safe;responsibility of thread safety is upon the caller
 */
class AuditService {

  public static final String WINDOW_SIZE_KEY = "audit.window.size.sec";
  public static final String AGGREGATE_WINDOW_KEY =
      "audit.aggregate.window.sec";
  public static final String AUDIT_ENABLED_KEY = "audit.enabled";
  private static final int DEFAULT_WINDOW_SIZE = 60;
  private static final int DEFAULT_AGGREGATE_WINDOW_SIZE = 60;
  private int windowSize;
  private int aggregateWindowSize;
  final HashMap<String, AuditCounterAccumulator> topicAccumulatorMap =
      new HashMap<String, AuditCounterAccumulator>();
  private final String tier = "publisher";
  private ScheduledThreadPoolExecutor executor;
  private boolean isInit = false;
  private AuditWorker worker;

  private static final Logger LOG = LoggerFactory.getLogger(AuditService.class);
  private AbstractMessagePublisher publisher;
  private String hostname;

  class AuditWorker implements Runnable {
    private final TSerializer serializer = new TSerializer();

    @Override
    public void run() {
      /*
       * synchronizing on publisher's instance to avoid execution of this block
       * via 2 threads at same time,this block can be executed via 2 thread
       * 1)through application's thread when close() is called 2) in
       * AuditService's thread and Also reset operation on accumulator is not
       * thread safe
       */

      synchronized (publisher) {
        try {
          LOG.info("Running the AuditWorker");
          for (Entry<String, AuditCounterAccumulator> entry : topicAccumulatorMap
              .entrySet()) {
            String topic = entry.getKey();
            AuditCounterAccumulator accumulator = entry.getValue();
            Counters counters = accumulator.getAndReset();
            if (counters.received.size() == 0 && counters.sent.size() == 0) {
              LOG.info("Not publishing audit packet as all the metric counters are"
                  + " 0");
              continue;
            }
            AuditMessage packet =
                createPacket(topic, counters.received, counters.sent);
            publishPacket(packet);

          }
        } catch (Throwable e) {// catching general exception so that thread
                               // should
                               // not get aborted
          LOG.error("Error while publishing the audit message", e);
        }
      }

    }

    private void publishPacket(AuditMessage packet) {
      try {
        LOG.debug("Publishing audit packet" + packet);
        publisher.publish(AuditUtil.AUDIT_STREAM_TOPIC_NAME, new Message(
            ByteBuffer.wrap(serializer.serialize(packet))));
      } catch (TException e) {
        LOG.error("Error while serializing the audit packet " + packet, e);
      }
    }

    private AuditMessage createPacket(String topic, Map<Long, Long> received,
        Map<Long, Long> sent) {
      long currentTime = new Date().getTime();
      AuditMessage packet =
          new AuditMessage(currentTime, topic, tier, hostname, windowSize,
              received, sent, null, null);
      return packet;
    }

    public void flush() {
      run();
    }

  }

  AuditService(AbstractMessagePublisher publisher) {
    this.publisher = publisher;
  }

  void init() throws IOException {
    if (isInit)
      return;
    init(new ClientConfig());
  }

  void init(ClientConfig config) throws IOException {
    if (isInit)
      return;
    windowSize = config.getInteger(WINDOW_SIZE_KEY, DEFAULT_WINDOW_SIZE);
    aggregateWindowSize =
        config.getInteger(AGGREGATE_WINDOW_KEY, DEFAULT_AGGREGATE_WINDOW_SIZE);
    executor = new ScheduledThreadPoolExecutor(1);
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to find the hostanme of the local box,audit packets won'"
          + "t contain hostname");
      hostname = "";
    }
    worker = new AuditWorker();
    executor.scheduleWithFixedDelay(worker, aggregateWindowSize,
        aggregateWindowSize, TimeUnit.SECONDS);
    // setting init flag to true
    isInit = true;
  }

  private AuditCounterAccumulator getAccumulator(String topic) {
    if (!topicAccumulatorMap.containsKey(topic))
      topicAccumulatorMap.put(topic, new AuditCounterAccumulator(windowSize));
    return topicAccumulatorMap.get(topic);
  }

  void close() {
    if (worker != null) {
      worker.flush(); // flushing the last audit packet during shutdown
      topicAccumulatorMap.clear();
    }
    if (executor != null) {
      executor.shutdown();
    }
  }

  void incrementReceived(String topicName, Long timestamp) {
    AuditCounterAccumulator accumulator = getAccumulator(topicName);
    LOG.debug("Just before Incremetning for topic [" + topicName
        + "] in audit service");
    accumulator.incrementReceived(timestamp);
    LOG.debug("Just After Incremetning for topic [" + topicName
        + "] in audit service");
  }

}
