package com.inmobi.messaging.metrics;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2014 InMobi
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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CollectorReaderStatsExposer extends PartitionReaderStatsExposer {

  private static final String SWITCHES_FROM_COLLECTOR_TO_LOCAL =
      "switchesFromCollectorToLocal";
  private static final String SWITCHES_FROM_LOCAL_TO_COLLECTOR =
      "switchesFromLocalToCollector";
  private static final String WAIT_TIME_UNITS_IN_SAME_FILE =
      "waitTimeUnitsInSameFile";

  private final AtomicLong numSwitchesFromCollectorToLocal = new AtomicLong(0);
  private final AtomicLong numSwitchesFromLocalToCollector = new AtomicLong(0);
  private final AtomicLong numWaitTimeUnitsInSameFile = new AtomicLong(0);

  public CollectorReaderStatsExposer(String topicName, String consumerName,
      String pid, int consumerNumber, String fsUri) {
    super(topicName, consumerName, pid, consumerNumber, fsUri);
  }

  public void incrementSwitchesFromCollectorToLocal() {
    numSwitchesFromCollectorToLocal.incrementAndGet();
  }

  public void incrementSwitchesFromLocalToCollector() {
    numSwitchesFromLocalToCollector.incrementAndGet();
  }

  public void incrementWaitTimeUnitsInSameFile() {
    numWaitTimeUnitsInSameFile.incrementAndGet();
  }

  @Override
  protected void addToStatsMap(Map<String, Number> map) {
    super.addToStatsMap(map);
    map.put(SWITCHES_FROM_COLLECTOR_TO_LOCAL, getSwitchesFromCollectorToLocal());
    map.put(SWITCHES_FROM_LOCAL_TO_COLLECTOR, getSwitchesFromLocalToCollector());
    map.put(WAIT_TIME_UNITS_IN_SAME_FILE, getWaitTimeInSameFile());
  }

  public long getSwitchesFromCollectorToLocal() {
    return numSwitchesFromCollectorToLocal.get();
  }

  public long getSwitchesFromLocalToCollector() {
    return numSwitchesFromLocalToCollector.get();
  }

  public long getWaitTimeInSameFile() {
    return numWaitTimeUnitsInSameFile.get();
  }
}
