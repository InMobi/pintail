package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.BaseMessageConsumerStatsExposer;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestDatabusConsumerWithFailover extends
    TestAbstractDatabusConsumer {
  DatabusConsumer testConsumer;
  private int numClusters = 1;
  private int numCollectors = 1;
  private String streamName = "testclient";

  @Override
  ClientConfig loadConfig() {
    return ClientConfig
        .loadFromClasspath("messaging-consumer-conf4.properties");
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c4";
    collectors = new String[] { "collector1" };
    dataFiles = new String[] { TestUtil.files[0], TestUtil.files[1],
        TestUtil.files[2], TestUtil.files[3] };
    super.setup(3);
  }

  @Test
  public void testLocalStream() throws Exception {
    ClientConfig config1 = loadConfig(), config2 = loadConfig();
    config1.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString());
    config2.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[1].toString());
    config1.set(DatabusConsumerConfig.checkpointDirConfig,
        rootDirs[0].toString()+"/databustest5/checkpoint1");
    config2.set(DatabusConsumerConfig.checkpointDirConfig,
        rootDirs[0].toString()+"/databustest5/checkpoint1");
    config1.set(DatabusConsumerConfig.databusStreamType,
        StreamType.LOCAL.name());
    config2.set(DatabusConsumerConfig.databusStreamType,
        StreamType.LOCAL.name());
    assertMessages(config1, config2);
  }

  @Test
  public void testMergedStream() throws Exception {
    ClientConfig config1 = loadConfig(), config2 = loadConfig();
    config1.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString());
    config2.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[1].toString());
    config1.set(DatabusConsumerConfig.checkpointDirConfig,
        rootDirs[0].toString()+"/databustest6/checkpoint1");
    config2.set(DatabusConsumerConfig.checkpointDirConfig,
        rootDirs[0].toString()+"/databustest6/checkpoint1");
    config1.set(DatabusConsumerConfig.databusStreamType,
        StreamType.MERGED.name());
    config2.set(DatabusConsumerConfig.databusStreamType,
        StreamType.MERGED.name());
    assertMessages(config1, config2);
  }

  private void assertMessages(ClientConfig config1, ClientConfig config2)
      throws Exception {
    int numCounters = numClusters * numCollectors;
    int totalMessages = numCounters * numDataFiles * numMessagesPerFile;
    int[] counter = new int[numCounters];
    for (int i = 0; i < numCounters; i++) {
      counter[i] = 0;
    }
    int[] markedcounter1 = new int[numCounters];
    int[] markedcounter2 = new int[numCounters];

    AbstractMessagingDatabusConsumer consumer = new DatabusConsumer();
    consumer.init(streamName, consumerName, null, config1);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);

    for (int i = 0; i < totalMessages / 2; i++) {
      Message msg = consumer.next();
      String msgStr = new String(msg.getData().array());
      for (int m = 0; m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(counter[m]))) {
          counter[m]++;
          break;
        }
      }
    }
    consumer.mark();
    Checkpoint lastCheckpoint = new Checkpoint(consumer.getCurrentCheckpoint()
        .toBytes());

    for (int i = 0; i < numCounters; i++) {
      markedcounter1[i] = counter[i];
      markedcounter2[i] = counter[i];
    }

    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (consumer
        .getMetrics())).getNumMarkCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (consumer
        .getMetrics())).getNumResetCalls(), 0);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (consumer
        .getMetrics())).getNumMessagesConsumed(), (totalMessages / 2));

    // restart consumer with different rootDir
    consumer = new DatabusConsumer();
    consumer.init(streamName, consumerName, null, config2);
    Assert.assertEquals(consumer.getCurrentCheckpoint(), lastCheckpoint);
    for (int i = 0; i < totalMessages / 2; i++) {
      Message msg = consumer.next();
      String msgStr = new String(msg.getData().array());
      for (int m = 0; m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(markedcounter2[m]))) {
          markedcounter2[m]++;
          break;
        }
      }
    }

    for (int i = 0; i < numCounters; i++) {
      Assert.assertEquals(markedcounter2[i], numDataFiles * numMessagesPerFile);
    }

    consumer.reset();
    for (int i = 0; i < totalMessages / 2; i++) {
      Message msg = consumer.next();
      String msgStr = new String(msg.getData().array());
      for (int m = 0; m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(markedcounter1[m]))) {
          markedcounter1[m]++;
          break;
        }
      }
    }

    for (int i = 0; i < numCounters; i++) {
      Assert.assertEquals(markedcounter1[i], numDataFiles * numMessagesPerFile);
    }

    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (consumer
        .getMetrics())).getNumMarkCalls(), 0);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (consumer
        .getMetrics())).getNumResetCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (consumer
        .getMetrics())).getNumMessagesConsumed(), (totalMessages));
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }
}
