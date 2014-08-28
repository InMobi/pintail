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

import com.inmobi.stats.StatsExposer;

/**
 * Provides access to client metrics and associated contexts.
 *
 * Provides ability for derived classes to add metrics and contexts.
 */
public abstract class AbstractMessagingClientStatsExposer implements
    StatsExposer {

  /**
   * Gets all the statistics associated with this metric object.
   *
   * @return a Map of counter name to its value
   */
  public Map<String, Number> getStats() {
    Map<String, Number> statsMap = new HashMap<String, Number>();
    addToStatsMap(statsMap);
    return statsMap;
  }

  /**
   * Gets all the contexts associated with this metrics object.
   *
   * @return a Map of context name to context value
   */
  public Map<String, String> getContexts() {
    Map<String, String> contexts = new HashMap<String, String>();
    addToContextsMap(contexts);
    return contexts;
  }

  /**
   * For every new metric added, please add the metric name and
   * getter to the map passed.
   */
  protected abstract void addToStatsMap(Map<String, Number> map);

  /**
   * For every new context needed, please add the context name and
   * value to the map.
   */
  protected abstract void addToContextsMap(Map<String, String> map);
}
