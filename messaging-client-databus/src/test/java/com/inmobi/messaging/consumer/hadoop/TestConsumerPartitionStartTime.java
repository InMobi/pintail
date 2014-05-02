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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

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
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.hadoop.HadoopConsumer;

public class TestConsumerPartitionStartTime {

  protected static final Log LOG = LogFactory.getLog(
      TestConsumerPartitionStartTime.class);
  HadoopConsumer consumer;
  HadoopConsumer secondConsumer;
  String firstConfFile = "messaging-consumer-hadoop-conf5.properties";
  String secondConfFile = "messaging-consumer-hadoop-conf6.properties";
  String streamName = "testStream";
  Date startTime;
  List<String> firstConsumedMessages;
  List<String> secondConsumedMessages;
  ClientConfig config;
  ClientConfig secondconfig;
  int numMessagesPerFile = 100;
  int numDataFiles;
  int numSuffixDirs;
  protected String[] dataFiles = new String[] {HadoopUtil.files[0],
      HadoopUtil.files[1],
      HadoopUtil.files[2], 
      HadoopUtil.files[3]};
  protected String[] suffixDirs;
  protected String consumerName;
  protected Path[] rootDirs;
  Path [][] finalPaths;
  Configuration conf;
  protected String chkpointPath;

  @BeforeTest
  public void setup() throws Exception {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MINUTE, -30);
    startTime = cal.getTime();
    config = ClientConfig.loadFromClasspath(firstConfFile);
    secondconfig = ClientConfig.loadFromClasspath(secondConfFile);
    firstConsumedMessages = new ArrayList<String>();
    secondConsumedMessages = new ArrayList<String>();
    createFiles(consumer);
    chkpointPath = config.getString(HadoopConsumerConfig.checkpointDirConfig);
    consumer = new HadoopConsumer();
    secondConsumer = new HadoopConsumer();
  }

  public void createFiles(HadoopConsumer consumer) throws Exception {
    consumer = new HadoopConsumer();
    consumer.initializeConfig(config);

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
  public void testWithStartTime() throws Exception {
    consumer.init(streamName, consumerName, startTime, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);

    secondConsumer.init(streamName, consumerName, startTime, secondconfig);
    Assert.assertEquals(secondConsumer.getTopicName(), streamName);
    Assert.assertEquals(secondConsumer.getConsumerName(), consumerName);
    Assert.assertEquals(secondConsumer.getStartTime(), consumer.getStartTime());

    //consume all messages
    while (firstConsumedMessages.size() < 600) {
      Message msg = consumer.next();
      firstConsumedMessages.add(getMessage(msg.getData().array()));
    }
    consumer.close();
    LOG.debug("msgs consumed by first consumer" + firstConsumedMessages.size());

    while (secondConsumedMessages.size() < 600) {
      Message msgs = secondConsumer.next();
      secondConsumedMessages.add(getMessage(msgs.getData().array()));
    }
    secondConsumer.close();
    LOG.debug("msgs consumed by second consumer " + secondConsumedMessages.size());

    Assert.assertEquals(firstConsumedMessages.size()
        + secondConsumedMessages.size(), 1200);
  }	

  @AfterTest
  public void cleanup() throws Exception {
    FileSystem lfs = FileSystem.getLocal(conf);
    for (Path rootDir : rootDirs) {
      LOG.debug("Cleaning up the dir: " + rootDir.getParent());
      lfs.delete(rootDir.getParent(), true);
    }
    lfs.delete(new Path(chkpointPath).getParent(), true);
  }

  private static String getMessage(byte[] array) throws IOException {
    return MessageUtil.getTextMessage(array).toString();
  }
}
