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
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ThreadedMapBenchmark;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestDatabusConsumer extends TestAbstractDatabusConsumer {

  ClientConfig loadConfig() {
    return ClientConfig.loadFromClasspath(
        MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c1";
    collectors = new String[] {COLLECTOR_PREFIX + "1"};
    dataFiles = new String[] {TestUtil.files[0], TestUtil.files[1],
        TestUtil.files[2]};
    super.setup(1);
  }

  @Test
  public void testTimeoutStats() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck1);
    ConsumerUtil.testTimeoutStats(config, testStream, consumerName,
        CollectorStreamReader.getDateFromCollectorFile(dataFiles[0]), false, 300);
  }

  @Test
  public void testMarkAndReset() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck5);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.testMarkAndReset(config, testStream, consumerName, false);
  }

  @Test
    public void testDynamicCollector() throws Exception {
      ClientConfig config = loadConfig();
      config.set(DatabusConsumerConfig.databusRootDirsConfig,
          rootDirs[0].toUri().toString());
      config.set(DatabusConsumerConfig.checkpointDirConfig, ck14);
      config.set(MessagingConsumerConfig.relativeStartTimeConfig,
          relativeStartTime);
       config.set(DatabusConsumerConfig.frequencyForDiscoverer, "1");
      ConsumerUtil.testDynamicCollector(config, testStream, consumerName, false,
          rootDirs, conf, testStream, COLLECTOR_PREFIX);
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
        rootDirs[0].toString() + "," + rootDirs[1].toString() + ","
            + rootDirs[2].toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck4);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    assertMessages(config, 3, 1);
  }

  @Test
  public void testConsumerStartUp() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck6);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.testConsumerStartUp(config, testStream,
        consumerName, false,
        CollectorStreamReader.getDateFromCollectorFile(dataFiles[1]),
        rootDirs[0], chkpointPathPrefix);
  }

  @Test
  public void testConsumerWithConfiguredStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck7);
    Date absoluteStartTime = CollectorStreamReader.
        getDateFromCollectorFile(dataFiles[1]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    ConsumerUtil.testConsumerWithConfiguredStartTime(config, false);
  }

  @Test
  public void testConsumerWithFutureStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    Date absoluteStartTime = CollectorStreamReader.
        getDateFromCollectorFile(dataFiles[1]);
    // created a future time stamp
    Calendar cal = new GregorianCalendar();
    cal.setTime(absoluteStartTime);
    cal.add(Calendar.HOUR, 2);

    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(cal.getTime()));
    ConsumerUtil.testConsumerWithFutureStartTime(config);
  }

  @Test
  public void testConsumerWithoutConfiguredOptions() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck8);
    ConsumerUtil.testConsumerWithoutConfiguredOptions(config);
  }

  @Test
  public void testConsumerWithRetentionPeriod() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck9);
    config.set(MessagingConsumerConfig.retentionConfig, "1");
    ConsumerUtil.testConsumerWithRetentionPeriod(config, testStream,
        consumerName, false);
  }

  /*
   *  setting retention period as 0 hours and relative time is 30 minutes.
   *  Consumer should start consume the messages from 30 minutes beyond the
   *  current time
   */
  @Test
  public void testConsumerWithRelativeAndRetention() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck10);
    config.set(DatabusConsumerConfig.retentionConfig, "0");
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    Date absoluteStartTime = CollectorStreamReader.
        getDateFromCollectorFile(dataFiles[1]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    ConsumerUtil.testConsumerWithRelativeAndRetention(config, testStream,
        consumerName, absoluteStartTime, false);
  }

  @Test
  public void testConsumerWithAbsoluteStartTimeAndRetention()
      throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.retentionConfig, "1");
    Date absoluteStartTime = CollectorStreamReader.
        getDateFromCollectorFile(dataFiles[1]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck11);
    ConsumerUtil.testConsumerWithAbsoluteStartTimeAndRetention(config,
        testStream, consumerName, absoluteStartTime, false);
  }

  @Test
  public void testConsumerWithStopTimeBeyondCheckpoint() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());

    FileSystem fs = rootDirs[0].getFileSystem(conf);
    try {
      // Deleting the dummy collector(COLLECTOR_PREFIX i.e. which does not have
      // any files to read).
      // Collector won't have any checkpoint if there are no files to read.
      // In this test, we wanted to test whether consumer is stopped if the
      // stop time is beyond the checkpoint.
      // If checkpoint is not present then consumer won't be closed completely.
      fs.delete(new Path(rootDirs[0].toUri().toString(),
          "data/" + testStream + "/" + COLLECTOR_PREFIX));
      Date absoluteStartTime = CollectorStreamReader.
          getDateFromCollectorFile(dataFiles[0]);
      config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
          AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
      config.set(DatabusConsumerConfig.checkpointDirConfig, ck12);
      Date stopDate = CollectorStreamReader.getDateFromCollectorFile(dataFiles[1]);
      Date stopDateForCheckpoint = CollectorStreamReader.
          getDateFromCollectorFile(dataFiles[0]);
      config.set(DatabusConsumerConfig.stopDateConfig,
          AbstractMessageConsumer.minDirFormat.get().format(stopDate));
      ConsumerUtil.testConsumerWithStopTimeBeyondCheckpoint(config,
          testStream, consumerName, absoluteStartTime, false, stopDateForCheckpoint);
    } finally {
      // create a dummy collector directory back
      fs.mkdirs(new Path(rootDirs[0].toUri().toString(),
          "data/" + testStream + "/" + COLLECTOR_PREFIX));
    }
  }

  @Test
  public void testConsumerWithStartOfStream() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(MessagingConsumerConfig.startOfStreamConfig, "true");
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck13);
    ConsumerUtil.testConsumerWithStartOfStream(config, testStream, consumerName,
        false);
  }

  @Test
  public void testDatabusConsumerBacklogOnlyCollector() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
            rootDirs[2].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck15);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
            relativeStartTime);
    config.set(DatabusConsumerConfig.frequencyForDiscoverer, "1");
    ConsumerUtil.testConsumerBacklogOnlyCollector(config, testStream , consumerName, false,
            rootDirs, conf, testStream, COLLECTOR_PREFIX);
  }

  @Test
  public void testDatabusConsumerBacklog() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
            rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck15);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
            relativeStartTime);
    config.set(DatabusConsumerConfig.frequencyForDiscoverer, "1");
    ConsumerUtil.testConsumerBacklog(config, testStream, consumerName, false,
            rootDirs, conf, testStream, COLLECTOR_PREFIX);
  }

  @Test
  public void testDatabusConsumerBacklog2() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
            rootDirs[0].toUri().toString());
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck15);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
            relativeStartTime);
    config.set(DatabusConsumerConfig.frequencyForDiscoverer, "1");
    ConsumerUtil.testConsumerBacklogMoreCollectors(config, testStream, consumerName, false,
            rootDirs, conf, testStream, COLLECTOR_PREFIX);
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }
}
