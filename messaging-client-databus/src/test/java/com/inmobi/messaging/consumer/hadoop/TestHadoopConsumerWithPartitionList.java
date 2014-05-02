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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.HadoopUtil;

public class TestHadoopConsumerWithPartitionList  {
  protected static final Log LOG = LogFactory.getLog(
      TestHadoopConsumerWithPartitionList.class);
  HadoopConsumer consumer;
  HadoopConsumer secondConsumer;
  String firstConfFile = "messaging-consumer-hadoop-conf11.properties";
  String secondConfFile = "messaging-consumer-hadoop-conf12.properties";
  String streamName = "testStream";
  int numMessagesPerFile = 100;
  int numDataFiles;
  int numSuffixDirs;
  protected String[] dataFiles = new String[] {HadoopUtil.files[0],
      HadoopUtil.files[1],
      HadoopUtil.files[2], HadoopUtil.files[3]};
  protected String[] suffixDirs;
  protected String consumerName = "c1";
  protected Path[] rootDirs;
  Path [][] finalPaths;
  Configuration conf;
  ClientConfig firstConsumerConfig;
  ClientConfig secondConsuemrConfig;
  private String ck1;

  boolean hadoop = true;

  @BeforeTest
  public void setup() throws Exception {
    firstConsumerConfig = ClientConfig.loadFromClasspath(firstConfFile);
    secondConsuemrConfig = ClientConfig.loadFromClasspath(secondConfFile);

    createFiles(consumer);
    ck1 = firstConsumerConfig.getString(
            HadoopConsumerConfig.checkpointDirConfig);
    consumer = new HadoopConsumer();
    secondConsumer = new HadoopConsumer();
  }

  public void createFiles(HadoopConsumer consumer) throws Exception {
    consumer = new HadoopConsumer();
    consumer.initializeConfig(firstConsumerConfig);

    conf = consumer.getHadoopConf();
    Assert.assertEquals(conf.get("myhadoop.property"), "myvalue");

    rootDirs = consumer.getRootDirs();
    numSuffixDirs = suffixDirs != null ? suffixDirs.length : 1;
    numDataFiles = dataFiles != null ? dataFiles.length : 1;
    finalPaths = new Path[rootDirs.length][numSuffixDirs * numDataFiles];
    for (int i = 0; i < rootDirs.length; i++) {
      HadoopUtil.setupHadoopCluster(
          conf, dataFiles, suffixDirs, finalPaths[i], rootDirs[i], false);
    }
    HadoopUtil.setUpHadoopFiles(rootDirs[0], conf,
        new String[] {"_SUCCESS", "_DONE"}, suffixDirs, null);
  }

  @Test
  public void testConsumerMarkAndResetWithStartTime() throws Exception {
    firstConsumerConfig.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[1].toString());
    firstConsumerConfig.set(HadoopConsumerConfig.checkpointDirConfig, ck1);
    secondConsuemrConfig.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[1].toString());
    secondConsuemrConfig.set(HadoopConsumerConfig.checkpointDirConfig, ck1);
    ConsumerUtil.testConsumerMarkAndResetWithStartTime(firstConsumerConfig,
        secondConsuemrConfig, streamName, consumerName,
        DatabusStreamWaitingReader.getDateFromStreamDir(
            rootDirs[1], finalPaths[0][1]), true);
  }

  @AfterTest
  public void cleanup() throws IOException {
    FileSystem lfs = FileSystem.getLocal(conf);
    for (Path rootDir : rootDirs) {
      LOG.debug("Cleaning up the dir: " + rootDir);
      lfs.delete(rootDir.getParent(), true);
    }
    // delete checkpoint dir
    lfs.delete(new Path(ck1).getParent(), true);
  }
}
