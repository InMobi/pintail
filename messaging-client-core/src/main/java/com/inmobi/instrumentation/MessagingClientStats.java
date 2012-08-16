package com.inmobi.instrumentation;

import java.io.IOException;
import java.util.Map;

import com.inmobi.stats.EmitterRegistry;
import com.inmobi.stats.StatsEmitter;
import com.inmobi.stats.StatsExposer;

/**
 * Wrapper class to initialize and close StatsEmitter
 */
public class MessagingClientStats {

  private StatsEmitter emitter;
  private boolean statEnabled = false;
  private StatsExposer statExposer;

  public boolean statEmissionEnabled() {
    return statEnabled;
  }

  public StatsEmitter getStatsEmitter() {
    return emitter;
  }

  /**
   * Initialize the emitter
   * 
   * @param configFileName Emitter configuration file name
   * @param statsMap Map containing all the metrics
   * @param contexts Map containing all the contexts associated with metrics
   * 
   * @throws IOException
   */
  public void init(String configFileName, final Map<String, Number> statsMap,
      final Map<String, String> contexts) throws IOException {
    try {
      emitter = EmitterRegistry.lookup(configFileName);
      statExposer = new StatsExposer() {
        @Override
        public Map<String, Number> getStats() {
          return statsMap;
        }
        @Override
        public Map<String, String> getContexts() {
          return contexts;
        }
      };
      emitter.add(statExposer);
      statEnabled = true;
    } catch (Exception e) {
      throw new IOException("Couldn't find or initialize the configured stats" +
          " emitter", e);
    }
  }

  /**
   * Closes the emitter
   */
  public void close() {
    if (emitter != null) {
      emitter.remove(statExposer);
    }
  }
}
