package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.SourceStream;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.util.MessageUtil;
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
    assertMessages(config, numClusters, numCollectors, numDataFiles);
  }
  void assertMessages(ClientConfig config, int numClusters, int numCollectors,
      int numDataFiles) throws IOException, InterruptedException {
    int numCounters = numClusters * numCollectors;
    int totalMessages = numCounters * numDataFiles * numMessagesPerFile;
    int[] counter = new int[numCounters];
    for (int i = 0; i <numCounters; i++) {
      counter[i] = 0;
    }
    int[] markedcounter = new int[numCounters];

    DatabusConsumer consumer = new DatabusConsumer();
    consumer.init(testStream, consumerName, null, config);
    Assert.assertEquals(consumer.getTopicName(), testStream);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);

    for (int i = 0; i < totalMessages/2; i++) {
      Message msg = consumer.next();
      String msgStr = new String(msg.getData().array());
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(counter[m]))) {
          counter[m]++;
          break;
        }
      }
    }
    consumer.mark();
    for (int i = 0; i < numCounters; i++) {
      markedcounter[i] = counter[i];
    }

    for (int i = 0; i < totalMessages/2; i++) {
      Message msg = consumer.next();
      String msgStr = new String(msg.getData().array());
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(counter[m]))) {
          counter[m]++;
          break;
        }
      }
    }    
    for (int i= 0; i < numCounters; i++) {
      Assert.assertEquals(counter[i], numDataFiles * numMessagesPerFile);
    }

    consumer.reset();

    for (int i = 0; i < totalMessages/2; i++) {
      Message msg = consumer.next();
      String msgStr = new String(msg.getData().array());
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(markedcounter[m]))) {
          markedcounter[m]++;
          break;
        }
      }
    }

    for (int i= 0; i < numCounters; i++) {
      Assert.assertEquals(markedcounter[i], numDataFiles * numMessagesPerFile);
    }
    consumer.close();
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
