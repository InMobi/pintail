package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.SourceStream;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumerFactory;

public class TestDatabusConsumer {
  private static final String testStream = "testclient";
  private static final String consumerName = "c1";
  private String[] collectors = new String[] {"collector1"};
  private String[] dataFiles = new String[] {TestUtil.files[0],
      TestUtil.files[1], TestUtil.files[2]};

  DatabusConsumer testConsumer;

  private ClientConfig loadConfig() {
    InputStream in = ClientConfig.class.getClassLoader().getResourceAsStream(
        MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
    if (in == null) {
      throw new RuntimeException("could not load conf file "
          + MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE 
          + " from classpath.");
    }
    return ClientConfig.load(in); 
  }

  @BeforeTest
  public void setup() throws IOException {

    ClientConfig config = loadConfig();
    testConsumer = new DatabusConsumer();
    testConsumer.initializeConfig(config);

    // setup stream, collector dirs and data files
    DatabusConfig databusConfig = testConsumer.getDatabusConfig();
    SourceStream sourceStream = 
        databusConfig.getSourceStreams().get(testStream);
    for (String c : sourceStream.getSourceClusters()) {
      Cluster cluster = databusConfig.getClusters().get(c);
      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      fs.delete(new Path(cluster.getRootDir()), true);
      Path streamDir = new Path(cluster.getDataDir(), testStream);
      fs.delete(streamDir, true);
      fs.mkdirs(streamDir);
      for (String collector : collectors) {
        Path collectorDir = new Path(streamDir, collector);
        fs.delete(collectorDir, true);
        fs.mkdirs(collectorDir);
        int i = 0;
        for (String file : dataFiles) {
          TestUtil.createMessageFile(file, fs, collectorDir, i);
          i += 100;
        }
      }
    }
  }

  @Test
  public void testMarkAndReset() throws Exception {
    ClientConfig config = loadConfig();
    config.set("databus.checkpoint.dir", "/tmp/databustest/checkpoint1");
    DatabusConsumer consumer = new DatabusConsumer();
    consumer.init(testStream, consumerName, config);
    Assert.assertEquals(consumer.getTopicName(), testStream);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);
    Map<PartitionId, PartitionReader> readers = consumer.getPartitionReaders();
    Assert.assertEquals(readers.size(), collectors.length);

    int i;
    for (i = 0; i < 20; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }
    consumer.mark(); 
    for (i = 20; i < 30; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }

    consumer.reset();

    for (i = 20; i < 140; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }

    consumer.mark();
    Checkpoint lastCheckpoint = new Checkpoint(
        consumer.getCurrentCheckpoint().toBytes());

    for (i = 140; i < 160; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }

    consumer.reset();
    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }

    consumer.close();

    // test checkpoint and consumer crash
    consumer = new DatabusConsumer();
    consumer.init(testStream, consumerName, config);
    Assert.assertEquals(consumer.getCurrentCheckpoint(), lastCheckpoint);

    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }
    consumer.mark();

    consumer.close();

  }

  @Test
  public void testMarkAndResetWithStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set("databus.checkpoint.dir", "/tmp/databustest/checkpoint2");
    DatabusConsumer consumer = new DatabusConsumer();
    consumer.init(testStream, consumerName,
        CollectorStreamReader.getDateFromCollectorFile(dataFiles[1]), config);
    Assert.assertEquals(consumer.getTopicName(), testStream);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);
    Map<PartitionId, PartitionReader> readers = consumer.getPartitionReaders();
    Assert.assertEquals(readers.size(), collectors.length);

    int i;
    for (i = 100; i < 120; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }
    consumer.mark(); 
    for (i = 120; i < 130; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }

    consumer.reset();

    for (i = 120; i < 240; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }

    consumer.mark();
    Checkpoint lastCheckpoint = new Checkpoint(
        consumer.getCurrentCheckpoint().toBytes());

    for (i = 240; i < 260; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }

    consumer.reset();
    for (i = 240; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }

    consumer.close();

    // test checkpoint and consumer crash
    consumer = new DatabusConsumer();
    consumer.init(testStream, consumerName, config);
    Assert.assertEquals(consumer.getCurrentCheckpoint(), lastCheckpoint);

    for (i = 240; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          constructMessage(i));
    }
    consumer.mark();

    consumer.close();
  }

  @AfterTest
  public void cleanup() throws IOException {
    testConsumer.close();
    DatabusConfig databusConfig = testConsumer.getDatabusConfig();
    SourceStream sourceStream = 
        databusConfig.getSourceStreams().get(testStream);
    for (String c : sourceStream.getSourceClusters()) {
      Cluster cluster = databusConfig.getClusters().get(c);
      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      fs.delete(new Path(cluster.getRootDir()), true);
    }
  }

  private String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    str.append(index).append("Message");
    return str.toString();
  }
}
