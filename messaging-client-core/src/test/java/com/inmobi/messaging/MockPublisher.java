package com.inmobi.messaging;

import java.util.Map;

import com.inmobi.instrumentation.TimingAccumulator.Outcome;

public class MockPublisher extends AbstractMessagePublisher {
  public static Message msg;
  public static void reset() {
    msg = null;
  }
  @Override
  protected void publish(Map<String, String> headers, Message m) {
    msg = m;
    getStats().accumulateOutcomeWithDelta(Outcome.SUCCESS, 0);
  }
}
