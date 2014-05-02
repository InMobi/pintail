package com.inmobi.messaging.consumer.hadoop;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2012 - 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;

public class TestHadoopConsumerWithSuffixDir extends TestAbstractHadoopConsumer {
  ClientConfig loadConfig() {
    return ClientConfig.loadFromClasspath(
        "messaging-consumer-hadoop-conf3.properties");
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c3";
    suffixDirs = new String[] {"xyz"};
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
