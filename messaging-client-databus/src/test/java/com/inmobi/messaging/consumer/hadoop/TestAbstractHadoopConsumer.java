package com.inmobi.messaging.consumer.hadoop;

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
import org.testng.annotations.Test;

import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.AbstractMessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.databus.DatabusConsumerConfig;
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

  int numMessagesPerFile = 100;
  int numDataFiles;
  int numSuffixDirs;
  HadoopConsumer testConsumer;
  static final String testStream = "testclient";
  protected String[] dataFiles = new String[]{HadoopUtil.files[0],
    HadoopUtil.files[1],
    HadoopUtil.files[2]};
  protected String[] suffixDirs;
  protected String consumerName;
  protected Path[] rootDirs;
  protected String[] chkDirs = new String[]{ck1, ck2, ck3, ck4, ck5, ck6, ck7,
      ck8};
  Path[][] finalPaths;
  Configuration conf;
  protected final String relativeStartTime = "20";

  abstract ClientConfig loadConfig();

  public void setup() throws Exception {
    // setup 
    ClientConfig config = loadConfig();
    testConsumer = new HadoopConsumer();
    testConsumer.initializeConfig(config);

    conf = testConsumer.getHadoopConf();
    Assert.assertEquals(conf.get("myhadoop.property"), "myvalue");

    rootDirs = testConsumer.getRootDirs();
    numSuffixDirs = suffixDirs != null ? suffixDirs.length : 1;
    numDataFiles = dataFiles != null ? dataFiles.length : 1;
    finalPaths = new Path[rootDirs.length][numSuffixDirs * numDataFiles];
    for (int i = 0; i < rootDirs.length; i++) {
      HadoopUtil.setupHadoopCluster(
        conf, dataFiles, suffixDirs, finalPaths[i], rootDirs[i]);
    }
    HadoopUtil.setUpHadoopFiles(rootDirs[0], conf,
      new String[]{"_SUCCESS", "_DONE"}, suffixDirs, null);
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
            rootDirs[0], finalPaths[0][0]), true);
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
      numSuffixDirs,
      numDataFiles, numMessagesPerFile, true);
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
      numSuffixDirs,
      numDataFiles, numMessagesPerFile, true);
  }

  public void testMultipleClusters2() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig, rootDirs[0].toString()
      + "," + rootDirs[1] + "," + rootDirs[2]);
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck5);
    config.set(MessagingConsumerConfig.relativeStartTimeConfig,
        relativeStartTime);
    ConsumerUtil.assertMessages(config, testStream, consumerName, 3,
      numSuffixDirs,
      numDataFiles, numMessagesPerFile, true);
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
        getDateFromStreamDir(rootDirs[0], finalPaths[0][1]), rootDirs[0]);
  }

  public void testConsumerWithConfiguredStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        rootDirs[0].toUri().toString());
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck8);
    Date absoluteStartTime = DatabusStreamWaitingReader.
        getDateFromStreamDir(rootDirs[0], finalPaths[0][1]);
    config.set(MessageConsumerFactory.ABSOLUTE_START_TIME,
        AbstractMessageConsumer.minDirFormat.get().format(absoluteStartTime));
    ConsumerUtil.testConsumerWithConfiguredStartTime(config, true);
  }

  public void testConsumerWithFutureStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusRootDirsConfig,
        rootDirs[0].toUri().toString());
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

  public void cleanup() throws IOException {
    FileSystem lfs = FileSystem.getLocal(conf);
    for (Path rootDir : rootDirs) {
      LOG.debug("Cleaning up the dir: " + rootDir.getParent());
      lfs.delete(rootDir.getParent(), true);
    }
    //Cleanup checkpoint directories, if we don't clean it up will cause tests to be flaky.
    for (String chk : chkDirs) {
      if (chk != null) {
        Path p = new Path(chk);
        if (lfs.exists(p)) {
          LOG.debug("Cleaning up the checkpoint dir: " + p);
          lfs.delete(p, true);
        }
      }
    }
  }
}
