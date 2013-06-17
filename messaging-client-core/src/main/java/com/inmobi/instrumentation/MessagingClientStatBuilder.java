package com.inmobi.instrumentation;

import java.io.IOException;

import com.inmobi.stats.EmitterRegistry;
import com.inmobi.stats.StatsEmitter;
import com.inmobi.stats.StatsExposer;

/**
 * Wrapper class to initialize and close StatsEmitter. Add more statExposers
 */
public class MessagingClientStatBuilder {

  private StatsEmitter emitter;
  private boolean statEnabled = false;

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
   *
   * @throws IOException
   */
  public void init(String configFileName) throws IOException {
    try {
      emitter = EmitterRegistry.lookup(configFileName);
      statEnabled = true;
    } catch (Exception e) {
      throw new IOException("Couldn't find or initialize the configured stats"
          + " emitter", e);
    }
  }

  /**
   * Add the statsExposer to the emitter
   *
   * @param statsExposer
   */
  public void add(StatsExposer statsExposer) {
    if (statEnabled) {
      emitter.add(statsExposer);
    }
  }

  public void remove(StatsExposer statsExposer) {
    if (statEnabled) {
      emitter.remove(statsExposer);
    }
  }
}
