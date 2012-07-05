package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.SourceStream;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public abstract class TestAbstractDatabusConsumer {

  int numMessagesPerFile = 100;
  int numDataFiles = 3;
  DatabusConsumer testConsumer;
  static final String testStream = "testclient";
  protected String[] collectors;
  protected String[] dataFiles;
  protected String consumerName;


  public void setup(int numFileToMove) throws Exception {

    ClientConfig config = loadConfig();
    testConsumer = new DatabusConsumer();
    testConsumer.initializeConfig(config);

    // setup stream, collector dirs and data files
    DatabusConfig databusConfig = testConsumer.getDatabusConfig();
    SourceStream sourceStream = 
        databusConfig.getSourceStreams().get(testStream);
    for (String c : sourceStream.getSourceClusters()) {
      Cluster cluster = databusConfig.getClusters().get(c);
      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      fs.delete(new Path(cluster.getRootDir()), true);
      Path streamDir = new Path(cluster.getDataDir(), testStream);
      fs.delete(streamDir, true);
      fs.mkdirs(streamDir);
      for (String collector : collectors) {
        Path collectorDir = new Path(streamDir, collector);
        fs.delete(collectorDir, true);
        fs.mkdirs(collectorDir);
        TestUtil.setUpFiles(cluster, collector, dataFiles, null, null,
            numFileToMove, numFileToMove);
      }
    }
  }

  abstract ClientConfig loadConfig();

  void assertMessages(
      ClientConfig config, int numClusters, int numCollectors) 
      throws IOException, InterruptedException {
    ConsumerUtil.assertMessages(config, testStream, consumerName, numClusters, numCollectors,
        numDataFiles, 100, false);
  }

  public void cleanup() throws IOException {
    testConsumer.close();
    DatabusConfig databusConfig = testConsumer.getDatabusConfig();
    SourceStream sourceStream = 
        databusConfig.getSourceStreams().get(testStream);
    for (String c : sourceStream.getSourceClusters()) {
      Cluster cluster = databusConfig.getClusters().get(c);
      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      fs.delete(new Path(cluster.getRootDir()), true);
    }
  }

}
