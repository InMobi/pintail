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

  @Test
  public void testConsumerWithAbsoluteStartTimeAndStopTime() throws Exception {
    super.testConsumerWithAbsoluteStartTimeAndStopTime();
  }

  @Test
  public void testConsumerWithAbsoluteStopTime() throws Exception {
    super.testConsumerWithAbsoluteStopTime();
  }

  @Test
  public void testConsumerWithStopTimeBeyondCheckpoint() throws Exception {
    super.testConsumerWithStopTimeBeyondCheckpoint();
  }

  @Test
  public void testConsumerWithStartOfStream() throws Exception {
    super.testConsumerWithStartOfStream();
  }

  @Test
  public void testConsumerStartOfStreamWithStopTime() throws Exception {
    super.testConsumerStartOfStreamWithStopTime();
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }
}
