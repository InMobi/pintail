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
