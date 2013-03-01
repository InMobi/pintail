package com.inmobi.messaging.publisher;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is not thread safe in respect to increments and reset
 * Responsibility is upon the caller
 * 
 * @author rohit.kochar
 * 
 */
public class AuditCounterAccumulator {
  private static final Logger LOG = LoggerFactory
      .getLogger(AuditCounterAccumulator.class);
  private Counters counters = new Counters(new HashMap<Long, Long>(),
      new HashMap<Long, Long>());
  private int windowSize;

  class Counters {
    private HashMap<Long, Long> received;
    private HashMap<Long, Long> sent;

    public Map<Long, Long> getReceived() {
      return received;
    }

    public Map<Long, Long> getSent() {
      return sent;
    }

    Counters(HashMap<Long, Long> received, HashMap<Long, Long> sent) {
      this.received = received;
      this.sent = sent;
    }
  }

  AuditCounterAccumulator(int windowSize) {

    this.windowSize = windowSize;
  }

  private Long getWindow(Long timestamp) {
    Long window = timestamp - (timestamp % (windowSize * 1000));
    return window;
  }

  void incrementReceived(Long timestamp) {
    Long window = getWindow(timestamp);
    if (!counters.received.containsKey(window)) {
      counters.received.put(window, new Long(0));
    }
    counters.received.put(window, counters.received.get(window) + 1);
  }

  void incrementSent(Long timestamp) {
    Long window = getWindow(timestamp);
    if (!counters.sent.containsKey(window)) {
      counters.sent.put(window, new Long(0));
    }
    counters.sent.put(window, counters.sent.get(window) + 1);

  }

  Counters getAndReset() {
    Counters returnValue;
    returnValue = new Counters(counters.received, counters.sent);
    counters.received = new HashMap<Long, Long>();
    counters.sent = new HashMap<Long, Long>();
    return returnValue;
  }
}
