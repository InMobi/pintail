package com.inmobi.messaging.instrumentation;

/*
 * #%L
 * messaging-client-core
 * %%
 * Copyright (C) 2012 - 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Store a cumulative stats for invocation of some piece of code.
 *
 * Note that only usage convention governs the semantics of values stored in
 * here. This is not meant to be AOP style code that does the actual magic.
 *
 * (cumulativeNanoseconds / successCount) can be used to figure out mean time
 * spent under normal circumstances i.e. free of unhandled exceptions
 */
public class PintailTimingAccumulator {

  private final AtomicLong invocationCount = new AtomicLong(0);

  private final AtomicLong cumulativeNanoseconds = new AtomicLong(0);

  public enum Outcome {
    SUCCESS,
    GRACEFUL_FAILURE,
    UNHANDLED_FAILURE,
    LOST,
    RETRY,
    REJECT,
    EXCEEDED_MSG_SIZE
  }

  private final AtomicLong successCount = new AtomicLong(0);
  private final AtomicLong gracefulTerminates = new AtomicLong(0);
  private final AtomicLong failureCount = new AtomicLong(0);
  private final AtomicLong retryCount = new AtomicLong(0);
  private final AtomicLong rejectCount = new AtomicLong(0);
  private final AtomicLong lostCount = new AtomicLong(0);
  private final AtomicLong reconnectCount = new AtomicLong(0);
  private final AtomicLong exceededMsgSizeCount = new AtomicLong(0);

  /**
   * The number of times something was invoked.
   * Increment this counter at entry.
   */
  public void accumulateInvocation() {
    invocationCount.incrementAndGet();
  }

  public long accumulateInvocationStartTimer() {
    long r = System.nanoTime();
    invocationCount.incrementAndGet();
    return r;
  }

  public void accumulateReconnections() {
    reconnectCount.incrementAndGet();
  }

  /**
   * Accumulator for time spent in a call
   * Usually incremented only on successful returns
   */
  private void accumulateSuccess() {
    successCount.incrementAndGet();
  }

  private void accumulateFailure() {
    failureCount.incrementAndGet();
  }

  private void accumulateRetry() {
    retryCount.incrementAndGet();
  }

  private void accumulateReject() {
    rejectCount.incrementAndGet();
  }


  private void accumulateLost() {
    lostCount.incrementAndGet();
  }

  private void accumulateExceededMsgSize() {
    exceededMsgSizeCount.incrementAndGet();
  }

  /**
   * The number of times something returned without an unhandled exception.
   * Increment this count at exit
   */
  private void accumulateTimeSpent(long nanos) {
    cumulativeNanoseconds.addAndGet(nanos);
  }

  private void accumulateGracefulTerminates() {
    gracefulTerminates.incrementAndGet();
  }

  public void accumulateOutcomeWithDelta(Outcome o, long delta) {
    accumulateOutcome(o);
    accumulateTimeSpent(delta);
  }

  public void accumulateOutcome(Outcome o, long startTime) {
    accumulateOutcome(o);
    long e = System.nanoTime();
    accumulateTimeSpent(e - startTime);
  }

  private void accumulateOutcome(Outcome o) {
    switch(o) {
    case SUCCESS:
      accumulateSuccess();
      break;
    case GRACEFUL_FAILURE:
      accumulateGracefulTerminates();
      break;
    case UNHANDLED_FAILURE:
      accumulateFailure();
      break;
    case LOST:
      accumulateLost();
      break;
    case RETRY:
      accumulateRetry();
      break;
    case REJECT:
      accumulateReject();
      break;
    case EXCEEDED_MSG_SIZE:
      accumulateExceededMsgSize();
      break;
    }
  }

  public long getInvocationCount() {
    return invocationCount.get();
  }

  public long getSuccessCount() {
    return successCount.get();
  }

  public long getLostCount() {
    return lostCount.get();
  }

  public long getRetryCount() {
    return retryCount.get();
  }

  public long getRejectCount() {
    return rejectCount.get();
  }

  public long getReconnectionCount() {
    return reconnectCount.get();
  }
  public long getCumulativeNanoseconds() {
    return cumulativeNanoseconds.get();
  }

  public long getUnhandledExceptionCount() {
    return failureCount.get();
  }

  public long getGracefulTerminates() {
    return gracefulTerminates.get();
  }

  public long getExceededMsgSizeCount() {
    return exceededMsgSizeCount.get();
  }

  public long getInFlight() {
    /* We can either choose to maintain yet another variable
     * for counting any form of returns or add all the return counts.
     *
     * Having another variable implies yet another atomic increment
     * Not having implies a sloppy answer.
     *
     * We choose the latter since by definition, this is a shaky metric.
     *
     * As long as callers code accumulateInvocation() with exactly one
     * accumulateOutcome()/accumulateOutcomeDelta() following it, the
     * result shall remain non-negative.
     *
     * Since this is a gauge and not a running counter, by definition,
     * the values is allowed to fluctuate across readings in a busy system
     */

    return getInvocationCount() - (getSuccessCount() + getLostCount()
        + getGracefulTerminates() + getRejectCount());
  }

  @Override
  public String toString() {
    return String.format(" {\"nanos\": %d, \"invocations\": %d, \"success\": "
        + "%d, \"failures\": %d, \"terminates\": %d, \"in-flight\": %d,"
        + "  \"lost\": %d, \"retries\": %d, \"reconnections\": %d, \"reject\": %d} ",
        getCumulativeNanoseconds(), getInvocationCount(), getSuccessCount(),
        getUnhandledExceptionCount(), getGracefulTerminates(), getInFlight(),
        getLostCount(), getRetryCount(), getReconnectionCount(), getRejectCount());
  }

  public Map<String, Number> getMap() {
    HashMap<String, Number> hash = new HashMap<String, Number>();
    hash.put("cumulativeNanoseconds", getCumulativeNanoseconds());
    hash.put("invocationCount", getInvocationCount());
    hash.put("successCount", getSuccessCount());
    hash.put("unhandledExceptionCount",
        getUnhandledExceptionCount());
    hash.put("gracefulTerminates", getGracefulTerminates());
    hash.put("inFlight", getInFlight());
    hash.put("lost", getLostCount());
    hash.put("retryCount", getRetryCount());
    hash.put("reconnects", getReconnectionCount());
    hash.put("exceededMsgSizeCount", getExceededMsgSizeCount());
    hash.put("reject", getRetryCount());
    return hash;
  }
}
