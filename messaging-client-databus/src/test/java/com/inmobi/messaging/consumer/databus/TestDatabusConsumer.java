package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
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
  private String[] dataFiles = new String[] {"1", "2", "3"};
  private int msgIndex = 0;
  DatabusConsumer testConsumer;
  ClientConfig config;
  
  @BeforeSuite
  public void setup() throws IOException {
    InputStream in = ClientConfig.class.getClassLoader().getResourceAsStream(
              MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
    if (in == null) {
      throw new RuntimeException("could not load conf file "
     + MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE + " from classpath.");
    }
    config = ClientConfig.load(in);
    
    testConsumer = new DatabusConsumer();
    testConsumer.initializeConfig(config);

    // setup stream, collector dirs and data files
    DatabusConfig databusConfig = testConsumer.getDatabusConfig();
    SourceStream sourceStream = databusConfig.getSourceStreams().get(testStream);
    for (String c : sourceStream.getSourceClusters()) {
      Cluster cluster = databusConfig.getClusters().get(c);
      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      Path streamDir = new Path(cluster.getDataDir(), testStream);
      fs.mkdirs(streamDir);
      for (String collector : collectors) {
        msgIndex = 0;
        Path collectorDir = new Path(streamDir, collector);
        fs.mkdirs(collectorDir);
        for (String fileNum : dataFiles) {
          FSDataOutputStream out = fs.create(new Path(collectorDir, fileNum));
          for (int i = 0; i < 100; i++) {
            out.write(Base64.encodeBase64(constructMessage(msgIndex).getBytes()));
            out.write('\n');
            msgIndex++;
          }
          out.close();
        }
      }
    }
  }

  @Test
  public void testInitializeConfig() throws IOException {      
    DatabusConsumer consumer = new DatabusConsumer();
    Assert.assertTrue(consumer.isMarkSupported());
    
    consumer.initializeConfig(config);  
    Assert.assertNotNull(consumer.getCheckpointProvider());
    Assert.assertNull(consumer.getCurrentCheckpoint());
    Assert.assertNotNull(consumer.getDatabusConfig());
    Assert.assertEquals(consumer.getBufferSize(), 800);
    
    consumer.initializeCheckpoint(testStream);
    Assert.assertNotNull(consumer.getCurrentCheckpoint());

  }
  
  @Test
  public void testMarkAndReset() throws IOException {
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
      Assert.assertEquals(new String(msg.getData().array()), constructMessage(i));
    }

    consumer.reset();

    for (i = 20; i < 140; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()), constructMessage(i));
    }

    consumer.mark();
    Checkpoint lastCheckpoint = new Checkpoint(consumer.getCurrentCheckpoint().toBytes());

    for (i = 140; i < 160; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()), constructMessage(i));
    }

    consumer.reset();
    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()), constructMessage(i));
    }

    consumer.close();

    // test checkpoint and consumer crash
    consumer = new DatabusConsumer();
    consumer.init(testStream, consumerName, config);
    Assert.assertEquals(consumer.getCurrentCheckpoint(), lastCheckpoint);
    
    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()), constructMessage(i));
    }
    consumer.mark();

    consumer.close();
    
  }
  
  @AfterSuite
  public void cleanup() throws IOException {
    testConsumer.close();
    DatabusConfig databusConfig = testConsumer.getDatabusConfig();
    SourceStream sourceStream = databusConfig.getSourceStreams().get(testStream);
    for (String c : sourceStream.getSourceClusters()) {
      Cluster cluster = databusConfig.getClusters().get(c);
      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      fs.delete(cluster.getDataDir().getParent(), true);
    }
  }

  private String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    str.append(index).append("Message");
    return str.toString();
  }
}
