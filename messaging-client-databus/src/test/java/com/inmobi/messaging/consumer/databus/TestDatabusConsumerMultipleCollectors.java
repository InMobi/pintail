package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestDatabusConsumerMultipleCollectors
    extends TestAbstractDatabusConsumer {
  DatabusConsumer testConsumer;

  ClientConfig loadConfig() {
    return ClientConfig
        .loadFromClasspath("messaging-consumer-conf2.properties");
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c2";
    collectors = new String[] {"collector1", "collector2"};
    dataFiles = new String[] {TestUtil.files[0],
        TestUtil.files[1], TestUtil.files[2], TestUtil.files[3]};
    super.setup(3);
  }

  @Test
  public void testMergeStream() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck1);
    config.set(DatabusConsumerConfig.databusStreamType,
        StreamType.MERGED.name());
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    assertMessages(config, 1, 2);
  }

  @Test
  public void testMergeStreamMultipleClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString() + "," + rootDirs[1].toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck2);
    config.set(DatabusConsumerConfig.databusStreamType,
        StreamType.MERGED.name());
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    Throwable th = null;
    try {
      DatabusConsumer consumer = new DatabusConsumer();
      consumer.init(testStream, consumerName, null, config);
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

  @Test
  public void testLocalStream() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck3);
    config.set(DatabusConsumerConfig.databusStreamType,
        StreamType.LOCAL.name());
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    assertMessages(config, 1, 2);
  }

  @Test
  public void testLocalStreamMultipleClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString() + "," + rootDirs[1].toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck4);
    config.set(DatabusConsumerConfig.databusStreamType,
        StreamType.LOCAL.name());
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    assertMessages(config, 2, 2);
  }

  @Test
  public void testLocalStreamAllClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString() + "," + rootDirs[1].toString() + ","
            + rootDirs[2].toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck5);
    config.set(DatabusConsumerConfig.databusStreamType,
        StreamType.LOCAL.name());
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    assertMessages(config, 3, 2);
  }

  @Test
  public void testCollectorStream() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck6);
    config.set(DatabusConsumerConfig.databusStreamType,
        StreamType.COLLECTOR.name());
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.assertMessages(config, testStream, consumerName, 1, 2, 4, 100,
        false);
  }

  @Test
  public void testCollectorStreamMultipleClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString() + "," + rootDirs[1].toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck7);
    config.set(DatabusConsumerConfig.databusStreamType,
        StreamType.COLLECTOR.name());
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.assertMessages(config, testStream, consumerName, 2, 2, 4, 100,
        false);
  }

  @Test
  public void testCollectorStreamAllClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toString()+ "," + rootDirs[1].toString() + ","
            + rootDirs[2].toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck8);
    config.set(DatabusConsumerConfig.databusStreamType,
        StreamType.COLLECTOR.name());
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.assertMessages(config, testStream, consumerName, 3, 2, 4, 100,
        false);
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }

}
