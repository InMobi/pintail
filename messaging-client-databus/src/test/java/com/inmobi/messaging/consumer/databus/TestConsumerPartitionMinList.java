package com.inmobi.messaging.consumer.databus;

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
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;

public class TestConsumerPartitionMinList {

  protected static final Log LOG = LogFactory.getLog(
      TestConsumerPartitionMinList.class);
  int consumerId = 1;
  int totalNumberOfConsumers = 2;
  Set<Integer> expectedPartitionMinList;
  DatabusConsumer testConsumer;
  private String chkpointPath;

  @BeforeTest
  public void setup() throws Exception {
    ClientConfig config = ClientConfig.loadFromClasspath(
        "messaging-consumer-conf16.properties");

    expectedPartitionMinList = new TreeSet<Integer>();
    testConsumer = new DatabusConsumer();
    testConsumer.initializeConfig(config);
    chkpointPath = config.getString(DatabusConsumerConfig.checkpointDirConfig);
    if (totalNumberOfConsumers > 0 && consumerId > 0) {
      expectedPartitionMinList();
    }
  }

  public void expectedPartitionMinList() throws Exception {
    for (int i = 0; i < 60; i++) {
      if ((i % totalNumberOfConsumers) == (consumerId - 1)) {
        expectedPartitionMinList.add(i);
      }
    }
  }

  @Test
  public void testPartitionMinList() {
    Set<Integer> actualPartitionMinList = testConsumer.getPartitionMinList();
    Assert.assertEquals(consumerId, testConsumer.consumerNumber);
    Assert.assertEquals(totalNumberOfConsumers, testConsumer.totalConsumers);
    Assert.assertEquals(expectedPartitionMinList.size(), 
        actualPartitionMinList.size());
    expectedPartitionMinList.containsAll(actualPartitionMinList);
  }

  @AfterTest
  public void cleanUp() throws IOException {
    testConsumer.close();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.delete(new Path(chkpointPath).getParent(), true);
  }

}
