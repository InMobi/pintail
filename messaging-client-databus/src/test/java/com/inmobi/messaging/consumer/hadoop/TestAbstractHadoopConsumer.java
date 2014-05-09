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
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.databus.MessagingConsumerConfig;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.HadoopUtil;

public abstract class TestAbstractHadoopConsumer {
  static final Log LOG = LogFactory.getLog(TestAbstractHadoopConsumer.class);
  protected String ck1;
  protected String ck2;
  protected String ck3;
  protected String ck4;
  protected String ck5;
  protected String ck6;
  protected String ck7;
  protected String ck8;
  protected String ck9;
  protected String ck10;
  protected String ck11;
  protected String ck12;
  protected String ck13;
  protected String ck14;
  protected String ck15;
  protected String ck16;
  protected String ck17;
  protected String ck18;
  protected String ck19;
  protected String chkpointPathPrefix;

  int numMessagesPerFile = 100;
  int numDataFiles;
  int numSuffixDirs;
  HadoopConsumer testConsumer;
  static final String testStream = "testclient";
  protected String[] dataFiles = new String[]{HadoopUtil.files[0],
    HadoopUtil.files[1], HadoopUtil.files[3], HadoopUtil.files[4],
    HadoopUtil.files[6]};
  protected String[] suffixDirs;
  protected String consumerName;
  protected Path[] rootDirs;
  Path[][] finalPaths;
  Configuration conf;
  protected final String relativeStartTime = "90";
  protected boolean createFilesInNextHour = true;

  abstract ClientConfig loadConfig();

  public void setup() throws Exception {
    // setup
    ClientConfig config = loadConfig();
    testConsumer = new HadoopConsumer();
    testConsumer.initializeConfig(config);
    chkpointPathPrefix = config.getString(HadoopConsumerConfig.checkpointDirConfig);
    setUpCheckpointPaths();
    conf = testConsumer.getHadoopConf();
    Assert.assertEquals(conf.get("myhadoop.property"), "myvalue");

    rootDirs = testConsumer.getRootDirs();
    numSuffixDirs = suffixDirs != null ? suffixDirs.length : 1;
    numDataFiles = dataFiles != null ? dataFiles.length : 1;
    if (createFilesInNextHour) {
      finalPaths = new Path[rootDirs.length][numSuffixDirs * numDataFiles * 2];
    } else {
      finalPaths = new Path[rootDirs.length][numSuffixDirs * numDataFiles];
    }
    for (int i = 0; i < rootDirs.length; i++) {
      HadoopUtil.setupHadoopCluster(conf, dataFiles, suffixDirs,
          finalPaths[i], rootDirs[i], true, createFilesInNextHour);
    }
    HadoopUtil.setUpHadoopFiles(rootDirs[0], conf,
        new String[]{"_SUCCESS", "_DONE"}, suffixDirs, null);
  }

  private void setUpCheckpointPaths() {
    ck1 = new Path(chkpointPathPrefix, "checkpoint1").toString();
    ck2 = new Path(chkpointPathPrefix, "checkpoint2").toString();
    ck3 = new Path(chkpointPathPrefix, "checkpoint3").toString();
    ck4 = new Path(chkpointPathPrefix, "checkpoint4").toString();
    ck5 = new Path(chkpointPathPrefix, "checkpoint5").toString();
    ck6 = new Path(chkpointPathPrefix, "checkpoint6").toString();
    ck7 = new Path(chkpointPathPrefix, "checkpoint7").toString();
    ck8 = new Path(chkpointPathPrefix, "checkpoint8").toString();
    ck9 = new Path(chkpointPathPrefix, "checkpoint9").toString();
    ck10 = new Path(chkpointPathPrefix, "checkpoint10").toString();
    ck11 = new Path(chkpointPathPrefix, "checkpoint11").toString();
    ck12 = new Path(chkpointPathPrefix, "checkpoint12").toString();
    ck13 = new Path(chkpointPathPrefix, "checkpoint13").toString();
    ck14 = new Path(chkpointPathPrefix, "checkpoint14").toString();
    ck15 = new Path(chkpointPathPrefix, "checkpoint15").toString();
    ck16 = new Path(chkpointPathPrefix, "checkpoint16").toString();
    ck17 = new Path(chkpointPathPrefix, "checkpoint17").toString();
    ck18 = new Path(chkpointPathPrefix, "checkpoint18").toString();
    ck19 = new Path(chkpointPathPrefix, "checkpoint19").toString();
  }

  public void testMarkAndReset() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck1);
    config.set(HadoopConsumerConfig.rootDirsConfig,
      rootDirs[0].toString());
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.testMarkAndReset(config, testStream, consumerName, true);
  }

  public void testTimeoutStats() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck6);
    config.set(HadoopConsumerConfig.rootDirsConfig,
      rootDirs[0].toString());
    ConsumerUtil.testTimeoutStats(config, testStream, consumerName,
        DatabusStreamWaitingReader.getDateFromStreamDir(
            rootDirs[0], finalPaths[0][0]), true, 600);
  }

  public void testMarkAndResetWithStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck2);
    config.set(HadoopConsumerConfig.rootDirsConfig,
      rootDirs[0].toString());
    ConsumerUtil.testMarkAndResetWithStartTime(config, testStream, consumerName,
      DatabusStreamWaitingReader.getDateFromStreamDir(
        rootDirs[0], finalPaths[0][1]), true);
  }

  public void testSuffixDirs() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
      rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig,
      ck3);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.assertMessages(config, testStream, consumerName, 1,
      numSuffixDirs, 6, numMessagesPerFile, true);
  }


  public void testMultipleClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
      rootDirs[0].toString() + "," + rootDirs[1].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig,
      ck4);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.assertMessages(config, testStream, consumerName, 2,
      numSuffixDirs, 6, numMessagesPerFile, true);
  }

  public void testMultipleClusters2() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig, rootDirs[0].toString()
      + "," + rootDirs[1] + "," + rootDirs[2]);
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck5);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.assertMessages(config, testStream, consumerName, 3,
      numSuffixDirs, 6, numMessagesPerFile, true);
  }

  public void testConsumerStartUp() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck7);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.testConsumerStartUp(config, testStream, consumerName, true,
        DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][1]), rootDirs[0],
        chkpointPathPrefix);
  }

  public void testConsumerWithConfiguredStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck8);
    Date absoluteStartTime = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][1]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    ConsumerUtil.testConsumerWithConfiguredStartTime(config, true);
  }

  public void testConsumerWithFutureStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    Date absoluteStartTime = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][1]);
    // created a future time stamp
    Calendar cal = new GregorianCalendar();
    cal.setTime(absoluteStartTime);
    cal.add(Calendar.HOUR, 2);

    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(cal.getTime()));
    ConsumerUtil.testConsumerWithFutureStartTime(config);
  }

  public void testConsumerWithoutConfiguredOptions() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck9);
    ConsumerUtil.testConsumerWithoutConfiguredOptions(config);
  }

  public void testConsumerWithRetentionPeriod() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck10);
    config.set(HadoopConsumerConfig.retentionConfig, "2");
    ConsumerUtil.testConsumerWithRetentionPeriod(config, testStream,
        consumerName, true);
  }

  /*
   *  setting retention period as 0 hours and relative time is 90 minutes.
   *  Consumer should start consume the messages from 90 minutes beyond the
   *  current time
   */
  public void testConsumerWithRelativeAndRetention() throws Exception {
    ClientConfig config = ClientConfig.loadFromClasspath(
        "messaging-consumer-hadoop-conf-relative.properties");
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck11);
    config.set(HadoopConsumerConfig.retentionConfig, "0");
    Date absoluteStartTime = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][1]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    ConsumerUtil.testConsumerWithRelativeAndRetention(config, testStream,
        consumerName, absoluteStartTime, true);
  }

  public void testConsumerWithAbsoluteStartTimeAndRetention()
      throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck12);
    config.set(HadoopConsumerConfig.retentionConfig, "2");
    Date absoluteStartTime = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][1]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    ConsumerUtil.testConsumerWithAbsoluteStartTimeAndRetention(config,
        testStream, consumerName, absoluteStartTime, true);
  }

  public void testConsumerWithAbsoluteStartTimeAndStopTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck13);
    Date absoluteStartTime = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][0]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    Date stopDate = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][2]);
    config.set(HadoopConsumerConfig.stopDateConfig,
        AbstractMessageConsumer.minDirFormat.get().format(stopDate));
    ConsumerUtil.testConsumerWithAbsoluteStartTimeAndStopTime(config,
        testStream, consumerName, absoluteStartTime, true);
  }

  public void testConsumerWithAbsoluteStopTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck14);
    Date absoluteStartTime = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][0]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    Date stopDate = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][0]);
    config.set(HadoopConsumerConfig.stopDateConfig,
        AbstractMessageConsumer.minDirFormat.get().format(stopDate));
    ConsumerUtil.testConsumerWithAbsoluteStopTime(config,
        testStream, consumerName, absoluteStartTime, true);
  }

  public void testConsumerWithStopTimeBeyondCheckpoint() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck16);
    Date absoluteStartTime = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][0]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    Date stopDate = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][2]);
    Date stopDateForCheckpoint = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][0]);
    config.set(HadoopConsumerConfig.stopDateConfig,
        AbstractMessageConsumer.minDirFormat.get().format(stopDate));
    ConsumerUtil.testConsumerWithStopTimeBeyondCheckpoint(config,
        testStream, consumerName, absoluteStartTime, true, stopDateForCheckpoint);
  }

  public void testConsumerWithStartOfStream() throws Exception {
    ClientConfig config = loadConfig();
    config.set(MessagingConsumerConfig.startOfStreamConfig, "true");
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck17);
    ConsumerUtil.testConsumerWithStartOfStream(config, testStream, consumerName,
        true);
  }

  public void testConsumerStartOfStreamWithStopTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(MessagingConsumerConfig.startOfStreamConfig, "true");
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck18);
    Date stopDate = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][2]);
    config.set(HadoopConsumerConfig.stopDateConfig,
        AbstractMessageConsumer.minDirFormat.get().format(stopDate));
    ConsumerUtil.testConsumerStartOfStreamWithStopTime(config, testStream,
        consumerName, true);
  }

  public void testMarkAndResetWithStopTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig, rootDirs[0].toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck19);
    Date absoluteStartTime = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][0]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    Date stopDate = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][9]);
    config.set(HadoopConsumerConfig.stopDateConfig,
        AbstractMessageConsumer.minDirFormat.get().format(stopDate));
    ConsumerUtil.testMarkAndResetWithStopTime(config, testStream, consumerName,
        absoluteStartTime, true);
  }

  public void cleanup() throws IOException {
    FileSystem lfs = FileSystem.getLocal(conf);
    for (Path rootDir : rootDirs) {
      LOG.debug("Cleaning up the dir: " + rootDir.getParent());
      lfs.delete(rootDir.getParent(), true);
    }
    lfs.delete(new Path(chkpointPathPrefix).getParent(), true);
  }
}
