package com.inmobi.messaging.consumer.hadoop;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;

public class TestHadoopConsumerMultipleSuffixDirs
    extends TestAbstractHadoopConsumer {

  ClientConfig loadConfig() {
    return ClientConfig.loadFromClasspath(
        "messaging-consumer-hadoop-conf2.properties");
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c2";
    suffixDirs = new String[] {"xyz", "abc"};
    super.setup();
    Assert.assertEquals(rootDirs.length, 3);
  }

  @Test
  public void testSuffixDirs() throws Exception {
    super.testSuffixDirs();
  }

  @Test
  public void testMultipleClusters() throws Exception {
    super.testMultipleClusters();
  }

  @Test
  public void testMultipleClusters2() throws Exception {
    super.testMultipleClusters2();
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }
}
