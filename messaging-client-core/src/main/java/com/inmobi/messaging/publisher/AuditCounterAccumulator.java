package com.inmobi.messaging.publisher;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class Counters {
  ConcurrentHashMap<Long, AtomicLong> received;
  ConcurrentHashMap<Long, AtomicLong> sent;

  Counters(ConcurrentHashMap<Long, AtomicLong> received,
      ConcurrentHashMap<Long, AtomicLong> sent) {
    this.received = received;
    this.sent = sent;
  }
}

public class AuditCounterAccumulator {
  private Counters counters = new Counters(
      new ConcurrentHashMap<Long, AtomicLong>(),
      new ConcurrentHashMap<Long, AtomicLong>());
  private int windowSize;

  AuditCounterAccumulator(int windowSize) {

    this.windowSize = windowSize;
  }

  private Long getWindow(Long timestamp) {
    Long window = timestamp - (timestamp % (windowSize * 1000));
    return window;
  }

  synchronized void incrementReceived(Long timestamp) {
    Long window = getWindow(timestamp);
    if (!counters.received.containsKey(window)) {
      counters.received.putIfAbsent(window, new AtomicLong(0));
    }
    counters.received.get(window).incrementAndGet();

  }

  synchronized void incrementSent(Long timestamp) {
    Long window = getWindow(timestamp);
    if (!counters.sent.containsKey(window)) {
      counters.sent.putIfAbsent(window, new AtomicLong(0));
    }
    counters.sent.get(window).incrementAndGet();

  }

  synchronized Counters getAndReset() {
    Counters returnValue;
    returnValue = new Counters(counters.received, counters.sent);
    counters.received = new ConcurrentHashMap<Long, AtomicLong>();
    counters.sent = new ConcurrentHashMap<Long, AtomicLong>();
    return returnValue;
  }

}
