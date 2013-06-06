package com.inmobi.messaging.consumer.audit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;

/*
 * This class is responsible for launching multiple AuditStatsFeeder instances one per cluster
 */
public class AuditStats {

  private static final String CONF_PATH = "/usr/local/databus/conf/databus.xml";
  private static final String AUDIT_PATH_SUFFIX = "system/";
  private static final Logger LOG = LoggerFactory.getLogger(AuditStats.class);

  private void start(List<AuditStatsFeeder> feeders) throws Exception {
    DatabusConfigParser parser = new DatabusConfigParser(CONF_PATH);
    DatabusConfig config = parser.getConfig();
    for (Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      String rootDir = cluster.getValue().getRootDir() + File.separator
          + AUDIT_PATH_SUFFIX;
      AuditStatsFeeder feeder = new AuditStatsFeeder(cluster.getKey(), rootDir);
      feeders.add(feeder);
    }
    // start all feeders
    for (AuditStatsFeeder feeder : feeders) {
      LOG.info("starting feeder for cluster " + feeder.getClusterName());
      feeder.start();
    }
  }

  private void join(List<AuditStatsFeeder> feeders) {
    for (AuditStatsFeeder feeder : feeders) {
      feeder.join();
    }
  }

  public static void stop(List<AuditStatsFeeder> feeders) {

    try {
      LOG.info("Stoping Feeder...");
      for (AuditStatsFeeder feeder : feeders) {
        feeder.stop();
      }
    } catch (Exception e) {
      LOG.warn("Error in shutting down feeder", e);
    }

  }

  public static void main(String args[]) throws Exception{
    final List<AuditStatsFeeder> feeders = new ArrayList<AuditStatsFeeder>();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        AuditStats.stop(feeders);
      }
    });

    AuditStats stats = new AuditStats();
    try {
    stats.start(feeders);
    // wait for all feeders to finish
    stats.join(feeders);
    } finally {
      AuditStats.stop(feeders);
    }

  }
}
