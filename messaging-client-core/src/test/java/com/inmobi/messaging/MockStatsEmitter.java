package com.inmobi.messaging;

import java.util.Properties;

import com.inmobi.stats.StatsEmitterBase;

public class MockStatsEmitter extends StatsEmitterBase {
  public static boolean inited;

  static void reset() {
    inited = false;
  }
  @Override
  public void init(Properties props) {
    inited = true;
  }
  
}
