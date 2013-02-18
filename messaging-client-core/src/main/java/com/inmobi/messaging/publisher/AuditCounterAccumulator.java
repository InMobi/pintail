package com.inmobi.messaging.publisher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuditCounterAccumulator {
  private ConcurrentHashMap<Long, AtomicLong> received;
  private ConcurrentHashMap<Long, AtomicLong> sent;
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private int windowSize;

  AuditCounterAccumulator(int windowSize) {
    received = new ConcurrentHashMap<Long, AtomicLong>();
    sent = new ConcurrentHashMap<Long, AtomicLong>();
    this.windowSize = windowSize;
  }

  private Long getWindow(Long timestamp) {
    Long window = timestamp - (timestamp % (windowSize * 1000));
    return window;
  }

  void incrementReceived(Long timestamp) {
    Long window = getWindow(timestamp);
    lock.readLock().lock(); // to make sure that reset() and this method doesn't
                            // execute in parallel
    try {
      if (!received.containsKey(window)) {
        received.putIfAbsent(window, new AtomicLong(0));// without the preceding
      }
      received.get(window).incrementAndGet(); // check,a new object
                                              // would be formed for
                                              // each call to this
                                              // method
    } finally {
      lock.readLock().unlock();
    }
  }

  void incrementSent(Long timestamp) {
    Long window = getWindow(timestamp);
    lock.readLock().lock();
    try {
      if (!sent.containsKey(window)) {
        sent.putIfAbsent(window, new AtomicLong(0));
      }
      sent.get(window).incrementAndGet();
    } finally {
      lock.readLock().unlock();
    }
  }

  void reset() {
    lock.writeLock().lock();// only 1 thread should be resetting one instance of
                            // accumulator at a time
    try {
      received = new ConcurrentHashMap<Long, AtomicLong>();
      sent = new ConcurrentHashMap<Long, AtomicLong>();
    } finally {
      lock.writeLock().unlock();
    }
  }

  Map<Long, AtomicLong> getReceived() {
    lock.readLock().lock();
    try {
      return received;
    } finally {
      lock.readLock().unlock();
    }
  }

  Map<Long, AtomicLong> getSent() {
    lock.readLock().lock();
    try {
      return sent;
    } finally {
      lock.readLock().unlock();
    }
  }

}
