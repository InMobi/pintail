package com.inmobi.audit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuditCounterAccumulator {
  private static ConcurrentHashMap<Long, AtomicLong> received;
  private static ConcurrentHashMap<Long, AtomicLong> sent;
  private ReentrantReadWriteLock                     lock = new ReentrantReadWriteLock();
  private int windowSizeInMins;

  public AuditCounterAccumulator(int windowSizeInMins) {
    received = new ConcurrentHashMap<Long, AtomicLong>();
    sent = new ConcurrentHashMap<Long, AtomicLong>();
    this.windowSizeInMins = windowSizeInMins;
  }

  private Long getWindow(Long timestamp) {
    Long window = timestamp / (windowSizeInMins * 60 * 1000);
    return window;
  }

  public void incrementReceived(Long timestamp) {
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

  public void incrementSent(Long timestamp) {
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

  public void reset() {
    lock.writeLock().lock();// only 1 thread should be resetting at a time
    try {
    received = new ConcurrentHashMap<Long, AtomicLong>();
    sent = new ConcurrentHashMap<Long, AtomicLong>();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Map<Long, AtomicLong> getReceived() {
    lock.readLock().lock();
    try {
      return received;
    } finally {
      lock.readLock().unlock();
    }
  }

  public Map<Long, AtomicLong> getSent() {
    lock.readLock().lock();
    try {
      return sent;
    } finally {
      lock.readLock().unlock();
    }
  }

}
