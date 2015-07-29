package com.inmobi.messaging.consumer.util;

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
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.inmobi.databus.partition.PartitionReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;
import com.inmobi.messaging.consumer.BaseMessageConsumerStatsExposer;
import com.inmobi.messaging.consumer.EndOfStreamException;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.databus.AbstractMessagingDatabusConsumer;
import com.inmobi.messaging.consumer.databus.Checkpoint;
import com.inmobi.messaging.consumer.databus.CheckpointList;
import com.inmobi.messaging.consumer.databus.ConsumerCheckpoint;
import com.inmobi.messaging.consumer.databus.DatabusConsumer;
import com.inmobi.messaging.consumer.databus.DatabusConsumerConfig;
import com.inmobi.messaging.consumer.databus.MessagingConsumerConfig;
import com.inmobi.messaging.consumer.hadoop.HadoopConsumer;
import com.inmobi.messaging.consumer.hadoop.HadoopConsumerConfig;

public class ConsumerUtil {

  public static void createCheckpointList(ConsumerCheckpoint temp,
      Map<Integer, Checkpoint> checkpointMap,
      Map<PartitionId, PartitionCheckpoint> lastCheckpoint,
      AbstractMessagingDatabusConsumer consumer) throws Exception {
    if (temp instanceof CheckpointList) {
      //Do a deep copy of the Tree Map, as the entry sets in original map can 
      //change
      for (Map.Entry<Integer, Checkpoint> entry: ((CheckpointList) temp).
          getCheckpoints().entrySet()) {
        checkpointMap.put(entry.getKey(), new Checkpoint(entry.getValue().
            toBytes()));
      }
    } else {
      lastCheckpoint.putAll(((Checkpoint) temp).getPartitionsCheckpoint());
    }
  }

  public static void compareConsumerCheckpoints(ConsumerCheckpoint temp,
      Map<Integer, Checkpoint> checkpointMap,
      Map<PartitionId, PartitionCheckpoint> lastCheckpoint,
      AbstractMessagingDatabusConsumer consumer) {
    if (temp instanceof CheckpointList) {
      Assert.assertEquals(((CheckpointList) consumer.getCurrentCheckpoint()).
          getCheckpoints(), checkpointMap);
    } else {
      Assert.assertEquals(((Checkpoint) consumer.getCurrentCheckpoint()).
          getPartitionsCheckpoint(), lastCheckpoint);
    }
  }
  public static void assertMessages(ClientConfig config, String streamName,
      String consumerName, int numClusters, int numCollectors, int numDataFiles,
      int numMessagesPerFile, boolean hadoop)
          throws Exception {
    int numCounters = numClusters * numCollectors;
    int totalMessages = numCounters * numDataFiles * numMessagesPerFile;
    int[] counter = new int[numCounters];
    for (int i = 0; i < numCounters; i++) {
      counter[i] = 0;
    }
    int[] markedcounter1 = new int[numCounters];
    int[] markedcounter2 = new int[numCounters];
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);

    consumer.init(streamName, consumerName, null, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);

    for (int i = 0; i < totalMessages / 2; i++) {
      Message msg = consumer.next();
      String msgStr = getMessage(msg.getData().array(), hadoop);
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(counter[m]))) {
          counter[m]++;
          break;
        }
      }
    }
    consumer.mark();
    ConsumerCheckpoint temp = consumer.getCurrentCheckpoint();
    Map<PartitionId, PartitionCheckpoint> lastCheckpoint = new
        HashMap<PartitionId, PartitionCheckpoint>();
    Map<Integer, Checkpoint> checkpointMap = new HashMap<Integer, Checkpoint>();
    //create consumer checkpoint
    try {
      createCheckpointList(temp, checkpointMap, lastCheckpoint, consumer);
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (int i = 0; i < numCounters; i++) {
      markedcounter1[i] = counter[i];
      markedcounter2[i] = counter[i];
    }
    for (int i = 0; i < totalMessages / 2; i++) {
      Message msg = consumer.next();
      String msgStr = getMessage(msg.getData().array(), hadoop);
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(counter[m]))) {
          counter[m]++;
          break;
        }
      }
    }
    for (int i = 0; i < numCounters; i++) {
      Assert.assertEquals(counter[i], numDataFiles * numMessagesPerFile);
    }

    consumer.reset();

    for (int i = 0; i < totalMessages / 2; i++) {
      Message msg = consumer.next();
      String msgStr = getMessage(msg.getData().array(), hadoop);
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(markedcounter1[m]))) {
          markedcounter1[m]++;
          break;
        }
      }
    }

    for (int i = 0; i < numCounters; i++) {
      Assert.assertEquals(markedcounter1[i], numDataFiles * numMessagesPerFile);
    }
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMarkCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumResetCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(),
        (totalMessages + totalMessages / 2));
    compareConsumerCheckpoints(temp, checkpointMap, lastCheckpoint, consumer);
    // test checkpoint and consumer crash
    consumer = createConsumer(hadoop);
    if (numClusters == 1) {
      config.set(MessagingConsumerConfig.clustersNameConfig, "testCluster1");
    } else if (numClusters == 2) {
      config.set(MessagingConsumerConfig.clustersNameConfig, "testCluster1,testCluster2");
    }
    consumer.init(streamName, consumerName, null, config);

    for (int i = 0; i < totalMessages / 2; i++) {
      Message msg = consumer.next();
      String msgStr = getMessage(msg.getData().array(), hadoop);
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(markedcounter2[m]))) {
          markedcounter2[m]++;
          break;
        }
      }
    }

    for (int i = 0; i < numCounters; i++) {
      Assert.assertEquals(markedcounter2[i], numDataFiles * numMessagesPerFile);
    }

    consumer.mark();
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMarkCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumResetCalls(), 0);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(),
        (totalMessages / 2));
  }

  private static AbstractMessagingDatabusConsumer createConsumer(boolean hadoop) {
    if (hadoop) {
      return new HadoopConsumer();
    } else {
      return new DatabusConsumer();
    }
  }

  private static String getMessage(byte[] array, boolean hadoop)
      throws IOException {
    if (hadoop) {
      return MessageUtil.getTextMessage(array).toString();
    } else {
      return new String(array);
    }
  }

  public static void testMarkAndResetWithStartTime(ClientConfig config,
      String streamName, String consumerName, Date startTime, boolean hadoop)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);

    consumer.init(streamName, consumerName,
        startTime, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);


    int i;
    for (i = 100; i < 120; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    for (i = 120; i < 130; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();

    for (i = 120; i < 240; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.mark();
    ConsumerCheckpoint temp = consumer.getCurrentCheckpoint();
    Map<PartitionId, PartitionCheckpoint> lastCheckpoint = new
        HashMap<PartitionId, PartitionCheckpoint>();
    Map<Integer, Checkpoint> checkpointMap = new HashMap<Integer, Checkpoint>();
    //create consumer checkpoint
    createCheckpointList(temp, checkpointMap, lastCheckpoint, consumer);

    for (i = 240; i < 260; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();
    for (i = 240; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMarkCalls(), 2);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumResetCalls(), 2);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 230);

    // test checkpoint and consumer crash
    consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, startTime, config);
    compareConsumerCheckpoints(temp, checkpointMap, lastCheckpoint, consumer);
    for (i = 240; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();

    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMarkCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumResetCalls(), 0);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 60); 
  }

  public static void testConsumerStartUp(ClientConfig config,
      String streamName, String consumerName, boolean hadoop,
      Date absoluteStartTime, Path rootDir, String chkpointPathPrefix)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    // consumer config has both relative start time and absolute start time
    consumer.init(streamName, consumerName, absoluteStartTime, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);
    // consumer is starting from relative start time
    int i;
    for (i = 0; i < 120; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    for (i = 120; i < 130; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.reset();
    // consumer starts consuming messages from the checkpoint
    for (i = 120; i < 240; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 250);
    consumer = createConsumer(hadoop);
    config.set(MessagingConsumerConfig.clustersNameConfig, "testCluster");
    consumer.init(streamName, consumerName, absoluteStartTime, config);
    // consumer starts consuming messages from the checkpoint
    for (i = 120; i < 240; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    ConsumerCheckpoint temp = consumer.getCurrentCheckpoint();
    Map<PartitionId, PartitionCheckpoint> lastCheckpoint = new
        HashMap<PartitionId, PartitionCheckpoint>();
    Map<Integer, Checkpoint> checkpointMap = new HashMap<Integer, Checkpoint>();
    //create consumer checkpoint
    createCheckpointList(temp, checkpointMap, lastCheckpoint, consumer);
    for (i = 240; i < 260; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 140);
    consumer = createConsumer(hadoop);
    if (!hadoop) {
      config = ClientConfig.loadFromClasspath(
          MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
      config.set(DatabusConsumer.checkpointDirConfig,
          new Path(chkpointPathPrefix, "random-databus").toString());
      config.set(DatabusConsumerConfig.databusRootDirsConfig,
          rootDir.toUri().toString());
      config.set(MessagingConsumerConfig.clustersNameConfig, "testCluster");
    } else {
      config = ClientConfig.loadFromClasspath(
          "messaging-consumer-hadoop-conf.properties");
      config.set(HadoopConsumer.checkpointDirConfig,
          new Path(chkpointPathPrefix, "random-hadoop").toString());
      config.set(HadoopConsumerConfig.rootDirsConfig,
          rootDir.toString());
      config.set(MessagingConsumerConfig.clustersNameConfig, "testCluster");
    }

    // consumer starts from absolute start time
    consumer.init(streamName, consumerName, absoluteStartTime, config);
    for (i = 100; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 200);
  }

  public static void testTimeoutStats(ClientConfig config, String streamName,
      String consumerName, Date startTime, boolean hadoop, int numOfMessagaes)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, startTime, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);
    for (int i = 0; i < numOfMessagaes; i++) {
      consumer.next(60, TimeUnit.SECONDS);
    }
    consumer.mark();

    ConsumerCheckpoint expectedCheckpoint = consumer.getCurrentCheckpoint();
    Map<PartitionId, PartitionCheckpoint> lastCheckpoint = new
        HashMap<PartitionId, PartitionCheckpoint>();
    Map<Integer, Checkpoint> checkpointMap = new
        HashMap<Integer, Checkpoint>();
    //create consumer checkpoint
    createCheckpointList(expectedCheckpoint, checkpointMap, lastCheckpoint,
        consumer);
    for (int i = numOfMessagaes; i < numOfMessagaes + 10; i++) {
      consumer.next(1, TimeUnit.SECONDS);
    }
    compareConsumerCheckpoints(expectedCheckpoint, checkpointMap,
        lastCheckpoint, consumer);
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), numOfMessagaes);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer)(
        consumer.getMetrics())).getNumOfTiemOutsOnNext(), 10);
  }

  public static void testDynamicCollector(ClientConfig config, String streamName,
                                          String consumerName, boolean hadoop, Path[] rootDirs,
                                          Configuration conf, String testStream,
                                          String COLLECTOR_PREFIX) throws Exception {

    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);
    Assert.assertEquals(consumer.getPartitionReaders().size(), 2);

    //consume all the messages
    int i;
    for (i = 0; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    //add a collector
    FileSystem fs = rootDirs[0].getFileSystem(conf);
    Path collectorDir = new Path(rootDirs[0].toUri().toString(),
        "data/" + testStream + "/" + COLLECTOR_PREFIX + "8") ;
    fs.mkdirs(collectorDir);
    String dataFile = TestUtil.files[2];
    TestUtil.setUpCollectorDataFiles(fs, collectorDir, dataFile);
    //wait for the new messages to be consumed by the new partition readers
   // Thread.sleep(12000);
    for (i = 0; i < 100; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    fs.delete(collectorDir,true);
    consumer.close();
  }

  public static void testMarkAndReset(ClientConfig config, String streamName,
      String consumerName, boolean hadoop) throws Exception {

    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);
    if (hadoop) {
      Assert.assertEquals(consumer.getPartitionReaders().size(), 1);
    } else {
      // PartitionReader will be created for
      // Dummy Collector (one which has COLLECTOR_PREFIX subdirectory) also
      Assert.assertEquals(consumer.getPartitionReaders().size(), 2);
    }

    int i;
    for (i = 0; i < 20; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    for (i = 20; i < 30; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();
    for (i = 20; i < 140; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.mark();
    ConsumerCheckpoint temp = consumer.getCurrentCheckpoint();
    Map<PartitionId, PartitionCheckpoint> lastCheckpoint = new
        HashMap<PartitionId, PartitionCheckpoint>();
    Map<Integer, Checkpoint> checkpointMap = new
        HashMap<Integer, Checkpoint>();
    //create consumer checkpoint
    createCheckpointList(temp, checkpointMap, lastCheckpoint, consumer);

    for (i = 140; i < 160; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();
    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMarkCalls(), 2);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumResetCalls(), 2);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 330);

    // test checkpoint and consumer crash
    consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    compareConsumerCheckpoints(temp, checkpointMap, lastCheckpoint, consumer);
    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();

    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMarkCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumResetCalls(), 0);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 160);

  }

  public static void testConsumerMarkAndResetWithStartTime(ClientConfig config,
      ClientConfig secondConfig, String streamName, String consumerName,
      Date startTime, boolean hadoop)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    AbstractMessagingDatabusConsumer secondConsumer = createConsumer(hadoop);
    //first consumer initialization
    consumer.init(streamName, consumerName, startTime, config);
    //second consumer initialization
    secondConsumer.init(streamName, consumerName, startTime, secondConfig);

    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);
    Assert.assertEquals(consumer.getStartTime(), secondConsumer.getStartTime());

    int i;
    for (i = 0; i < 5; i++) {
      secondConsumer.next();
    }
    secondConsumer.mark();
    for (i = 0; i < 25; i++) {
      consumer.next();
    }
    consumer.mark();
    for (i = 5; i < 10; i++) {
      secondConsumer.next();
    }
    secondConsumer.reset();

    for (i = 5; i < 10; i++) {
      secondConsumer.next();
    }
    secondConsumer.mark();

    for (i = 25; i < 75; i++) {
      consumer.next();
    }
    consumer.mark();

    for (i = 75; i < 80; i++) {
      consumer.next();
    }

    consumer.reset();

    for (i = 75; i < 80; i++) {
      consumer.next();
    }
    consumer.mark();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 85);
    for (i = 80; i < 82; i++) {
      consumer.next();
    }

    consumer.reset();
    for (i = 80; i < 82; i++) {
      consumer.next();
    }
    consumer.mark();
    consumer.close();
    secondConsumer.close();

    ConsumerCheckpoint temp = consumer.getCurrentCheckpoint();
    //test checkpoint
    Map<Integer, Checkpoint> checkpointMap = new TreeMap<Integer, Checkpoint>();
    //create consumer checkpoint
    createCheckpointList(temp, checkpointMap, null, consumer);

    Assert.assertEquals(((CheckpointList) consumer.getCurrentCheckpoint()).
        getCheckpoints(), checkpointMap);

    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMarkCalls(), 4);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumResetCalls(), 2);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        secondConsumer.getMetrics())).getNumMarkCalls(), 2);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        secondConsumer.getMetrics())).getNumResetCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(),89);
  }

  public static void testConsumerWithConfiguredStartTime(ClientConfig config,
      boolean hadoop) throws Exception {
    AbstractMessagingDatabusConsumer consumer;
    if (!hadoop) {
      consumer = (DatabusConsumer) MessageConsumerFactory.create(config);
    } else {
      consumer = (HadoopConsumer) MessageConsumerFactory.create(config);
    }
    int i;
    for (i = 100; i < 200; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    for (i = 200; i < 220; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.reset();
    for (i = 200; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMarkCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumResetCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 220);
  }

  public static void testConsumerWithFutureStartTime(ClientConfig config)
      throws Exception {
    Throwable th = null;
    try {
      MessageConsumerFactory.create(config);
    } catch (Throwable e) {
      th = e;
    }
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

  public static void testConsumerWithoutConfiguredOptions(ClientConfig config)
      throws Exception {
    Throwable th = null;
    try {
      MessageConsumerFactory.create(config);
    } catch (Throwable e) {
      th = e;
    }
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

  public static void testConsumerWithRetentionPeriod(ClientConfig config,
      String streamName, String consumerName, boolean hadoop)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    int i;
    for (i = 0; i < 200; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 200);
    consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    for (i = 200; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 100);
  }

  public static void testConsumerWithRelativeAndRetention(ClientConfig config,
      String streamName, String consumerName, Date startTime, boolean hadoop)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, startTime, config);
    int i;
    for (i = 0; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 300);
  }

  public static void testConsumerWithAbsoluteStartTimeAndRetention(
      ClientConfig config, String streamName, String consumerName,
      Date startTime, boolean hadoop)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, startTime, config);
    int i;
    for (i = 0; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 300);
  }

  public static void testConsumerWithAbsoluteStartTimeAndStopTime(
      ClientConfig config, String streamName, String consumerName,
      Date startTime, boolean hadoop)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, startTime, config);
    int i;
    for (i = 0; i < 120; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 120);
    consumer.init(streamName, consumerName, startTime, config);
    for (i = 120; i < 200; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 80);
    Throwable th = null;
    try {
      consumer.next();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertTrue(th instanceof EndOfStreamException);
  }

  public static void testConsumerWithAbsoluteStopTime(
      ClientConfig config, String streamName, String consumerName,
      Date startTime, boolean hadoop)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, startTime, config);
    int i;
    for (i = 0; i < 20; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    for (i = 20; i < 40; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.reset();
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 40);
    consumer.init(streamName, consumerName, startTime, config);
    for (i = 20; i < 100; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 80);
    // throw an EndOfStreamException if consumer consumes one EOFMessage
    // (one partitionReader only)
    Throwable th = null;
    try {
      consumer.next();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertTrue(th instanceof EndOfStreamException);
    // throw an EndOfStreamException if user calls next() after consuming
    // all messages till stop time
    try {
      consumer.next();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertTrue(th instanceof EndOfStreamException);
  }

  public static void testConsumerWithStopTimeBeyondCheckpoint(
      ClientConfig config, String streamName, String consumerName,
      Date startTime, boolean hadoop, Date stopDate) throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, startTime, config);
    int i;
    for (i = 0; i < 20; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    for (i = 20; i < 200; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 200);
    config.set(HadoopConsumerConfig.stopDateConfig,
        AbstractMessageConsumer.minDirFormat.get().format(stopDate));
    consumer.init(streamName, consumerName, startTime, config);
    Throwable th = null;
    try {
      consumer.next();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertTrue(th instanceof EndOfStreamException);
  }

  public static void testConsumerWithStartOfStream(ClientConfig config,
      String streamName, String consumerName, boolean hadoop)
          throws IOException, InterruptedException, EndOfStreamException {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    int i;
    for (i = 0; i < 200; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    for (i = 200; i < 220; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.reset();
    for (i = 200; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.close();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMarkCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumResetCalls(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 320);
  }

  public static void testConsumerStartOfStreamWithStopTime(ClientConfig config,
      String streamName, String consumerName, boolean hadoop)
          throws IOException, InterruptedException, EndOfStreamException {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    int i;
    for (i = 0; i < 200; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 200);

    Throwable th = null;
    try {
      consumer.next();
    } catch (EndOfStreamException e) {
      th = e;
    }
    Assert.assertTrue(th instanceof EndOfStreamException);
    consumer.close();
  }

  public static void testMarkAndResetWithStopTime(ClientConfig config,
      String streamName, String consumerName, Date startTime, boolean hadoop)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, startTime, config);
    int i;
    for (i = 0; i < 20; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    for (i = 20; i < 100; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.reset();
    for (i = 20; i < 120; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    for (i = 120; i < 240; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.reset();
    for (i = 120; i < 600; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();
    Assert.assertEquals(((BaseMessageConsumerStatsExposer) (
        consumer.getMetrics())).getNumMessagesConsumed(), 800);
    Throwable th = null;
    try {
      consumer.next();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertTrue(th instanceof EndOfStreamException);
  }
}
