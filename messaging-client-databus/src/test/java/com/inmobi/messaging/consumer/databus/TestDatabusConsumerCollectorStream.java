package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestDatabusConsumerCollectorStream 
    extends TestAbstractDatabusConsumer {

  private String ck1 = "/tmp/test/databustest7/checkpoint12";
  private String ck2 = "/tmp/test/databustest7/checkpoint22";
  private String ck3 = "/tmp/test/databustest8/checkpoint12";
  private String ck4 = "/tmp/test/databustest8/checkpoint22";
  private String ck5 = "/tmp/test/databustest8/checkpoint32"; 

  ClientConfig loadConfig() {
    return ClientConfig
        .loadFromClasspath("messaging-consumer-conf3.properties");
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c3";
    collectors = new String[] {"collector1"};
    dataFiles = new String[] {TestUtil.files[0], TestUtil.files[1],
        TestUtil.files[2]};
    super.setup(0);
  }

  @Test
  public void testMarkAndReset() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck1);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.testMarkAndReset(config, testStream, consumerName, false);
  }

  @Test
  public void testMarkAndResetWithStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck2);
    ConsumerUtil.testMarkAndResetWithStartTime(config, testStream, consumerName,
        CollectorStreamReader.getDateFromCollectorFile(dataFiles[1]), false);
  }

  @Test
  public void testMultipleClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString() + "," + rootDirs[1].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig,
        ck3);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    assertMessages(config, 2, 1);
  }

  @Test
  public void testMultipleClusters2() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString() + "," + 
        rootDirs[1].toUri().toString() + "," + 
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck4);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    assertMessages(config, 3, 1);
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }

}
