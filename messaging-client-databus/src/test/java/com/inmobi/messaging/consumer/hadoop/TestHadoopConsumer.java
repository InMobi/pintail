package com.inmobi.messaging.consumer.hadoop;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;

public class TestHadoopConsumer extends TestAbstractHadoopConsumer {

  ClientConfig loadConfig() {
    return ClientConfig.loadFromClasspath(
        "messaging-consumer-hadoop-conf.properties");
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c1";
    ck1 = "/tmp/test/hadoop/1/checkpoint1";
    ck2 = "/tmp/test/hadoop/1/checkpoint2";
    ck3 = "/tmp/test/hadoop/1/checkpoint3";
    ck4 = "/tmp/test/hadoop/2/checkpoint1";
    ck5 = "/tmp/test/hadoop/2/checkpoint2";
    ck6 = "/tmp/test/hadoop/2/checkpoint3";
    ck7 = "/tmp/test/hadoop/2/checkpoint4";
    ck8 = "/tmp/test/hadoop/2/checkpoint5";
    ck9 = "/tmp/test/hadoop/2/checkpoint6";
    ck10 = "/tmp/test/hadoop/2/checkpoint7";
    super.setup();
    Assert.assertEquals(rootDirs.length, 3);
  }

  @Test
  public void testMarkAndReset() throws Exception {
    super.testMarkAndReset();
  }
 
  @Test
  public void testMarkAndResetWithStartTime() throws Exception {
    super.testMarkAndResetWithStartTime();
  }

  @Test
  public void testMultipleClusters() throws Exception {
    super.testMultipleClusters();
  }

  @Test
  public void testMultipleClusters2() throws Exception {
    super.testMultipleClusters2();
  }
  
  @Test
  public void testTimeoutStats() throws Exception {
    super.testTimeoutStats();
  }

  @Test
  public void testConsumerStartUp() throws Exception {
    super.testConsumerStartUp();
  }

  @Test
  public void testConsumerWithConfiguredStartTime() throws Exception {
    super.testConsumerWithConfiguredStartTime();
  }

  @Test
  public void testConsumerWithFutureStartTime() throws Exception {
    super.testConsumerWithFutureStartTime();
  }

  @Test
  public void testConsumerWithoutConfiguredOptions() throws Exception {
    super.testConsumerWithoutConfiguredOptions();
  }

  @Test
  public void testConsumerWithRetentionPeriod() throws Exception {
    super.testConsumerWithRetentionPeriod();
  }

  @Test
  public void testConsumerWithRelativeAndRetention() throws Exception {
    super.testConsumerWithRelativeAndRetention();
  }

  @Test
  public void testConsumerWithAbsoluteStartTimeAndRetention() throws Exception {
    super.testConsumerWithAbsoluteStartTimeAndRetention();
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }
}
