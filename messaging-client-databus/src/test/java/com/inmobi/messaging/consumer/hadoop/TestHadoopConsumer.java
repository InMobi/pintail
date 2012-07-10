package com.inmobi.messaging.consumer.hadoop;

import java.io.IOException;

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
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestHadoopConsumer {

  private String ck1 = "/tmp/test/hadoop/1/checkpoint1";
  private String ck2 = "/tmp/test/hadoop/1/checkpoint2";
  private String ck3 = "/tmp/test/hadoop/2/checkpoint1";
  private String ck4 = "/tmp/test/hadoop/2/checkpoint2";

  int numMessagesPerFile = 100;
  int numDataFiles = 3;
  HadoopConsumer testConsumer;
  static final String testStream = "testclient";
  protected String[] dataFiles;
  protected String consumerName;
  protected Path[] rootDirs;
  Path [][] finalPaths = new Path[3][3];
  Configuration conf;

  ClientConfig loadConfig() {
    return ClientConfig.loadFromClasspath(
        "messaging-consumer-hadoop-conf.properties");
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c1";
    dataFiles = new String[] {TestUtil.files[0], TestUtil.files[1],
        TestUtil.files[2]};
    // setup 
    ClientConfig config = loadConfig();    
    testConsumer = new HadoopConsumer();
    testConsumer.initializeConfig(config);

    conf = testConsumer.getHadoopConf();
    Assert.assertEquals(conf.get("myhadoop.property"), "myvalue");

    rootDirs = testConsumer.getRootDirs();
    Assert.assertEquals(rootDirs.length, 3);
    for (int i = 0; i < rootDirs.length; i++) {
      HadoopUtil.setupHadoopCluster(
          conf, dataFiles, finalPaths[i], new Path(rootDirs[i], testStream));
    }
  }

  @Test
  public void testMarkAndReset() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck1);
    config.set(HadoopConsumerConfig.rootDirsConfig,
        "file:///tmp/test/hadoop/1");
    ConsumerUtil.testMarkAndReset(config, testStream, consumerName, true);
  }

  @Test
  public void testMarkAndResetWithStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck2);
    config.set(HadoopConsumerConfig.rootDirsConfig,
        "file:///tmp/test/hadoop/1");
    ConsumerUtil.testMarkAndResetWithStartTime(config, testStream, consumerName,
        DatabusStreamWaitingReader.getDateFromStreamDir(
            new Path(rootDirs[0], testStream), finalPaths[0][1]), true);
  }

  @Test
  public void testMultipleClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig,
        "file:///tmp/test/hadoop/1,file:///tmp/test/hadoop/2");
    config.set(HadoopConsumerConfig.checkpointDirConfig,
        ck3);

    ConsumerUtil.assertMessages(config, testStream, consumerName, 2, 1,
        numDataFiles, numMessagesPerFile, true);
  }

  @Test
  public void testMultipleClusters2() throws Exception {
    ClientConfig config = loadConfig();
    config.set(HadoopConsumerConfig.rootDirsConfig, "file:///tmp/test/hadoop/1,"
    		+ "file:///tmp/test/hadoop/2,file:///tmp/test/hadoop/3");
    config.set(HadoopConsumerConfig.checkpointDirConfig, ck4);
    ConsumerUtil.assertMessages(config, testStream, consumerName, 3, 1,
        numDataFiles, numMessagesPerFile, true);
  }

  @AfterTest
  public void cleanup() throws IOException {
    FileSystem lfs = FileSystem.getLocal(conf);
    for (Path rootDir : rootDirs) {
      lfs.delete(rootDir, true);
    }
  }
}
