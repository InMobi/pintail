package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestDatabusConsumer extends TestAbstractDatabusConsumer {

  private String ck1 = "/tmp/test/databustest1/checkpoint1";
  private String ck2 = "/tmp/test/databustest1/checkpoint2";
  private String ck3 = "/tmp/test/databustest2/checkpoint1";
  private String ck4 = "/tmp/test/databustest2/checkpoint2";
  private String ck5 = "/tmp/test/databustest2/checkpoint3";

  ClientConfig loadConfig() {
    return ClientConfig.loadFromClasspath(
        MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c1";
    collectors = new String[] {"collector1"};
    dataFiles = new String[] {TestUtil.files[0], TestUtil.files[1],
        TestUtil.files[2]};
    super.setup(1);
  }

  @Test
  public void testMarkAndReset() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck1);
    ConsumerUtil.testMarkAndReset(config, testStream, consumerName, false);
  }

  @Test
  public void testMarkAndResetWithStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck2);
    ConsumerUtil.testMarkAndResetWithStartTime(config, testStream, consumerName,
        CollectorStreamReader.getDateFromCollectorFile(dataFiles[1]), false);
  }

  @Test
  public void testMultipleClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusClustersConfig,
        "testcluster1,testcluster2");
    config.set(DatabusConsumerConfig.checkpointDirConfig,
        ck3);
    assertMessages(config, 2, 1);
  }

  @Test
  public void testMultipleClusters2() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusClustersConfig,
        "testcluster1,testcluster2,testcluster3");
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck4);
    assertMessages(config, 3, 1);
  }

  @Test
  public void testMultipleClusters3() throws Exception {

    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusClustersConfig,
        null);
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck5);
    assertMessages( config, 3, 1);
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }
}
