package com.inmobi.messaging.metrics;

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

  public CollectorReaderStatsExposer(String pid) {
    super(pid);
  }

  public void addSwitchesFromCollectorToLocal() {
    numSwitchesFromCollectorToLocal.incrementAndGet();
  }

  public void addSwitchesFromLocalToCollector() {
    numSwitchesFromLocalToCollector.incrementAndGet();
  }

  public void addWaitTimeUnitsInSameFile() {
    numWaitTimeUnitsInSameFile.incrementAndGet();
  }

  @Override
  protected void addToStatsMap(Map<String, Number> map) {
    super.addToStatsMap(map);
    map.put(pidContextStr + SWITCHES_FROM_COLLECTOR_TO_LOCAL,
        getSwitchesFromCollectorToLocal());
    map.put(pidContextStr + SWITCHES_FROM_LOCAL_TO_COLLECTOR,
        getSwitchesFromLocalToCollector());
    map.put(pidContextStr + WAIT_TIME_UNITS_IN_SAME_FILE,
        getWaitTimeInSameFile());
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
