package com.inmobi.messaging.publisher;

import java.util.HashMap;
import java.util.Map;

import com.inmobi.audit.thrift.AuditMetrics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is not thread safe in respect to increments and reset
 * Responsibility is upon the caller
 *
 * @author rohit.kochar
 *
 */
public class AuditCounterAccumulator {
  private static final Log LOG = LogFactory.
      getLog(AuditCounterAccumulator.class);
  private Counters counters = new Counters(new HashMap<Long, Long>(),
      new HashMap<Long, Long>(), new HashMap<Long, AuditMetrics>(), new HashMap<Long, AuditMetrics>());
  private int windowSize;

  class Counters {
    //Map<Long, Long> received and sent will not be removed till feeder
    // change
    private HashMap<Long, Long> received;
    private HashMap<Long, Long> sent;
    private HashMap<Long, AuditMetrics> receivedMetrics;
    private HashMap<Long, AuditMetrics> sentMetrics;

    public Map<Long, Long> getReceived() {
      return received;
    }

    public Map<Long, Long> getSent() {
      return sent;
    }

    public Map<Long, AuditMetrics> getReceivedMetrics() {
      return receivedMetrics;
    }

    public Map<Long, AuditMetrics> getSentMetrics() {
      return sentMetrics;
    }

    Counters(HashMap<Long, Long> received, HashMap<Long, Long> sent,
             HashMap<Long, AuditMetrics> receivedMetrics, HashMap<Long,
        AuditMetrics> sentMetrics) {
      this(receivedMetrics, sentMetrics);
      this.received = received;
      this.sent = sent;
    }

    Counters(HashMap<Long, AuditMetrics> receivedMetrics, HashMap<Long,
        AuditMetrics> sentMetrics) {
      this.receivedMetrics = receivedMetrics;
      this.sentMetrics = sentMetrics;
    }

  }

  AuditCounterAccumulator(int windowSize) {
    this.windowSize = windowSize;
  }

  private Long getWindow(Long timestamp) {
    Long window = timestamp - (timestamp % (windowSize * 1000));
    return window;
  }

  void incrementReceived(Long timestamp, Long messageLength) {
    Long window = getWindow(timestamp);
    if (!counters.received.containsKey(window)) {
      counters.received.put(window, (long) 0);
    }
    counters.received.put(window, counters.received.get(window) + 1);

    AuditMetrics metrics;
    if (!counters.receivedMetrics.containsKey(window)) {
      metrics = new AuditMetrics(0, 0);
    } else {
      metrics = counters.receivedMetrics.get(window);
    }
    long newCount = metrics.getCount() + 1;
    long newSize = metrics.getSize() + messageLength;
    metrics.setCount(newCount);
    metrics.setSize(newSize);
    counters.receivedMetrics.put(window, metrics);
  }

  void incrementSent(Long timestamp, Long messageLength) {
    Long window = getWindow(timestamp);
    if (!counters.sent.containsKey(window)) {
      counters.sent.put(window, (long) 0);
    }
    counters.sent.put(window, counters.sent.get(window) + 1);

    AuditMetrics metrics;
    if (!counters.sentMetrics.containsKey(window)) {
      metrics = new AuditMetrics(0, 0);
    } else {
      metrics = counters.sentMetrics.get(window);
    }
    long newCount = metrics.getCount() + 1;
    long newSize = metrics.getSize() + messageLength;
    metrics.setCount(newCount);
    metrics.setSize(newSize);
    counters.sentMetrics.put(window, metrics);

  }

  Counters getAndReset() {
    Counters returnValue;
    returnValue = new Counters(counters.received, counters.sent,
        counters.receivedMetrics, counters.sentMetrics);
    counters.received = new HashMap<Long, Long>();
    counters.sent = new HashMap<Long, Long>();
    counters.receivedMetrics = new HashMap<Long, AuditMetrics>();
    counters.sentMetrics = new HashMap<Long, AuditMetrics>();
    return returnValue;
  }
}
