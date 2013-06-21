package com.inmobi.messaging.consumer.audit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.messaging.ClientConfig;

/*
 * This class is responsible for launching multiple AuditStatsFeeder instances one per cluster
 */
public class AuditStats {
  public static final String CONF_FILE = "audit-feeder.properties";
  private static final String DATABUS_CONF_FILE_KEY = "feeder.conf";
  private static final Log LOG = LogFactory.getLog(AuditStats.class);


  private synchronized void start(List<AuditStatsFeeder> feeders)
      throws Exception {
    ClientConfig config = ClientConfig.loadFromClasspath(CONF_FILE);
    String databusConf = config.getString(DATABUS_CONF_FILE_KEY);
    DatabusConfigParser parser = new DatabusConfigParser(databusConf);
    DatabusConfig dataBusConfig = parser.getConfig();
    for (Entry<String, Cluster> cluster : dataBusConfig.getClusters()
        .entrySet()) {
      String rootDir = cluster.getValue().getRootDir();
      AuditStatsFeeder feeder = new AuditStatsFeeder(cluster.getKey(), rootDir,
          config);
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

  public synchronized void stop(List<AuditStatsFeeder> feeders) {

    try {
      LOG.info("Stopping Feeder...");
      for (AuditStatsFeeder feeder : feeders) {
        LOG.info("Stopping feeder for cluster " + feeder.getClusterName());
        feeder.stop();
      }
      LOG.info("All feeders signalled to  stop");
    } catch (Exception e) {
      LOG.warn("Error in shutting down feeder", e);
    }

  }

  public static void main(String args[]) throws Exception{
    final AuditStats stats = new AuditStats();
    final List<AuditStatsFeeder> feeders = new ArrayList<AuditStatsFeeder>();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        stats.stop(feeders);
        stats.join(feeders);
        LOG.info("Finishing the shutdown hook");
      }
    });


    try {
    stats.start(feeders);
    // wait for all feeders to finish
    stats.join(feeders);
    } finally {
      stats.stop(feeders);
    }

  }
}
