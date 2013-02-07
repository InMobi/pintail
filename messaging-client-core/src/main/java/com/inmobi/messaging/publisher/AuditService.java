package com.inmobi.messaging.publisher;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class AuditService {

  private static final String WINDOW_SIZE_KEY = "window.size.sec";
  private static final String AGGREGATE_WINDOW_KEY = "aggregate.window.sec";
  private static final int DEFAULT_WINDOW_SIZE = 60;
  private static final int DEFAULT_AGGREGATE_WINDOW_SIZE = 60;
  private int windowSize;
  private int aggregateWindowSize;
  final ConcurrentHashMap<String, AuditCounterAccumulator> topicAccumulatorMap = new ConcurrentHashMap<String, AuditCounterAccumulator>();
  private final String tier = "publisher";
  private ScheduledThreadPoolExecutor executor;
  private boolean isInit = false;
  private AuditWorker worker;
  private static final byte[] magicBytes = { (byte) 0xAB, (byte) 0xCD,
      (byte) 0xEF };
  private static final int version = 1;
  private static final Logger LOG = LoggerFactory.getLogger(AuditService.class);
  private AbstractMessagePublisher publisher;

  AuditService(AbstractMessagePublisher publisher) {
    this.publisher = publisher;
  }

  public synchronized void init() throws IOException {
    if (isInit)
      return;
    init(new ClientConfig());
  }

  public synchronized void init(ClientConfig config) throws IOException {
    if (isInit)
      return;
    windowSize = config.getInteger(WINDOW_SIZE_KEY, DEFAULT_WINDOW_SIZE);
    aggregateWindowSize = config.getInteger(AGGREGATE_WINDOW_KEY,
        DEFAULT_AGGREGATE_WINDOW_SIZE);
    executor = new ScheduledThreadPoolExecutor(1);
    String hostname;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to find the hostanme of the local box,audit packets won't contain hostname");
      hostname = "";
    }
    worker = new AuditWorker(hostname, tier, windowSize, publisher,
        topicAccumulatorMap);
    executor.scheduleWithFixedDelay(worker, aggregateWindowSize,
        aggregateWindowSize, TimeUnit.SECONDS);
    // setting init flag to true
    isInit = true;
  }

  private AuditCounterAccumulator getAccumulator(String topic) {
    if (!topicAccumulatorMap.containsKey(topic))
      topicAccumulatorMap.putIfAbsent(topic, new AuditCounterAccumulator(
          windowSize));
    return topicAccumulatorMap.get(topic);
  }

  public synchronized void close() {
    if (executor != null) {
    executor.shutdown();
    }
  }

  public synchronized void flush() {
    if (worker != null) {
      worker.flush(); // flushing the last audit packet during shutdown
    }
  }

  public static void attachHeaders(Message m, Long timestamp) {
    byte[] b = m.getData().array();
    int messageSize = b.length;
    int totalSize = messageSize + 16;
    ByteBuffer buffer = ByteBuffer.allocate(totalSize);

    // writing version
    buffer.put((byte) version);
    // writing magic bytes
    buffer.put(magicBytes);
    // writing timestamp
    long time = timestamp;
    buffer.putLong(time);

    // writing message size
    buffer.putInt(messageSize);
    // writing message
    buffer.put(b);
    buffer.rewind();
    m.set(buffer);
    // return new Message(buffer);

  }

  public void incrementSent(String topicName, Long timestamp) {
    AuditCounterAccumulator accumulator = getAccumulator(topicName);
    accumulator.incrementSent(timestamp);
  }

}
