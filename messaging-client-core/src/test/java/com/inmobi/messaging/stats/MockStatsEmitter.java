package com.inmobi.messaging.stats;

import java.util.Properties;

import com.inmobi.stats.StatsEmitterBase;

public class MockStatsEmitter extends StatsEmitterBase {
  public boolean inited;

  public void reset() {
    inited = false;
  }
  @Override
  public void init(Properties props) {
    inited = true;
  }
  
}
