package com.inmobi.messaging.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import com.inmobi.stats.StatsEmitterBase;
import com.inmobi.stats.StatsExposer;

public class TextStatsEmitter extends StatsEmitterBase implements Runnable {

  private String statsPath;
  private Thread statsThread;
  private boolean should_run = true;
  private long sleep;
  private boolean appendStats = true;

  @Override
  public void init(Properties props) {
    sleep = Integer.valueOf(props.getProperty("text.emitter.poll.interval",
        "10000"));
    statsPath = props.getProperty("text.emitter.statspath", ".");
    appendStats = Boolean.parseBoolean(props.getProperty(
        "text.emitter.append.stats", "true"));
    createThread();
  }

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

  private void createThread() {
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

  private void writeStats(){
    Map<String, Number> stats;
    Map<String, String> contexts;
    try {
      synchronized(statsExposers) {
        for (StatsExposer exposer : statsExposers) {
          stats = exposer.getStats();
          contexts = exposer.getContexts();

          String statsFileName = statsPath + "/context";
          for (Map.Entry<String, String> pair: contexts.entrySet()) {
            statsFileName += "-" + pair.getKey() + "_" + pair.getValue();
          }
          statsFileName += ".txt";

          FileWriter fw = new FileWriter(statsFileName, appendStats);
          BufferedWriter out = new BufferedWriter(fw);

          out.write(new Date().toString());
          out.write("\n");
          for (Map.Entry<String, Number> stat: stats.entrySet()) {
            out.write(stat.getKey());
            out.write(":");
            out.write(stat.getValue().toString());
            out.write("\n");
          }
          out.close();
        }
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  @Override
  public void run() {
    while (should_run) {
      writeStats();
      try {
        Thread.sleep(this.sleep);
        // any signal can interrupt our sweet sleep
      } catch (InterruptedException e) {}
    }
  }
}