package com.inmobi.messaging.util;

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

import com.inmobi.stats.StatsEmitterBase;
import com.inmobi.stats.StatsExposer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public abstract class RunnableStatsEmitter extends StatsEmitterBase implements Runnable {

  protected Thread statsThread;
  protected boolean should_run = true;
  protected long sleep;

  @Override
  public synchronized void add(StatsExposer s) {
    super.add(s);
    start();
  }

  @Override
  public synchronized void remove(StatsExposer s) {
    writeStats();
    super.remove(s);
    if (isEmpty()) {
      stop();
    }
  }

  @Override
  public synchronized void removeAll() {
    writeStats();
    super.removeAll();
    stop();
  }

  protected void createThread() {
    statsThread = new Thread(this);
    statsThread.setName("statsThread");
  }

  protected void stop() {
    writeStats();
    if ((statsThread != null) && statsThread.isAlive()) {
      should_run = false;
      statsThread.interrupt();
    }
  }

  protected void start() {
    if (statsThread.getState() == Thread.State.TERMINATED) {
      createThread();
    }
    if ((statsThread != null) && !statsThread.isAlive()) {
      should_run = true;
      statsThread.start();
    }
  }

  protected abstract void writeStats();

  @Override
  public void run() {
    while (should_run) {
      writeStats();
      try {
        Thread.sleep(this.sleep);
        // any signal can interrupt our sweet sleep
      } catch (InterruptedException e) { }
    }
  }
}
