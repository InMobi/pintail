package com.inmobi.messaging;

import java.util.HashMap;
import java.util.Map;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.stats.EmitterRegistry;
import com.inmobi.stats.StatsEmitter;
import com.inmobi.stats.StatsExposer;

public abstract class AbstractMessagePublisher implements MessagePublisher {

  private final TimingAccumulator stats = new TimingAccumulator();
  private StatsEmitter emitter;
  private StatsExposer statExposer;
  private static final String HEADER_TOPIC = "topic";

  @Override
  public void publish(Message m) {
    getStats().accumulateInvocation();
    //TODO: generate headers
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(HEADER_TOPIC, m.getTopic());
    publish(headers, m);
  }

  protected abstract void publish(Map<String, String> headers,
      Message m);

  public TimingAccumulator getStats() {
    return stats;
  }

  @Override
  public void init(ClientConfig config) {
    try {
      String emitterConfig = null;//TODO; get from the classpath
      emitter = EmitterRegistry.lookup(emitterConfig);
      final Map<String, String> contexts = new HashMap<String, String>();
      contexts.put("messaging_type", "application");
      statExposer = new StatsExposer() {

        @Override
        public Map<String, Number> getStats() {
          HashMap<String, Number> hash = new HashMap<String, Number>();
          hash.put("cumulativeNanoseconds", stats.getCumulativeNanoseconds());
          hash.put("invocationCount", stats.getInvocationCount());
          hash.put("successCount", stats.getSuccessCount());
          hash.put("unhandledExceptionCount",
              stats.getUnhandledExceptionCount());
          hash.put("gracefulTerminates", stats.getGracefulTerminates());
          hash.put("inFlight", stats.getInFlight());
          return hash;
        }

        @Override
        public Map<String, String> getContexts() {
          return contexts;
        }
      };
      emitter.add(statExposer);
    } catch (Exception e) {
      System.err
          .println("Couldn't find or initialize the configured stats emitter");
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    emitter.remove(statExposer);
  }
}
