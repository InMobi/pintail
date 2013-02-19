package com.inmobi.messaging.publisher;

import java.util.HashMap;
import java.util.Map;

import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.AuditUtil;

public class MockPublisher extends AbstractMessagePublisher {
  private static Map<String, Message> msgs = new HashMap<String, Message>();

  public static void reset() {
    msgs.clear();
  }

  public static void reset(String topic) {
    msgs.remove(topic);
  }

  public static Message getMsg(String topic) {
    Message m =
        new Message(AuditUtil.removeHeader(msgs.get(topic).getData().array()));
    return m;
  }

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    String topic = headers.get(HEADER_TOPIC);
    msgs.put(topic, m);
    getStats(topic).accumulateOutcomeWithDelta(Outcome.SUCCESS, 0);
  }
}
