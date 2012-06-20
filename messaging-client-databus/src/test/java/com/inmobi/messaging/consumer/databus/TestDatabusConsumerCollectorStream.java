package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestDatabusConsumerCollectorStream 
    extends TestAbstractDatabusConsumer {

  private String ck1 = "/tmp/test/databustest7/checkpoint12";
  private String ck2 = "/tmp/test/databustest7/checkpoint22";
  private String ck3 = "/tmp/test/databustest8/checkpoint12";
  private String ck4 = "/tmp/test/databustest8/checkpoint22";
  private String ck5 = "/tmp/test/databustest8/checkpoint32";

  ClientConfig loadConfig() {
    ClientConfig config = ClientConfig.loadFromClasspath(
        MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
    config.set(DatabusConsumerConfig.databusConfigFileKey, "databus1.xml");
    return config;
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerName = "c3";
    collectors = new String[] {"collector1"};
    dataFiles = new String[] {TestUtil.files[0], TestUtil.files[1],
        TestUtil.files[2]};
    super.setup(0);
  }

  @Test
  public void testMarkAndReset() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck1);
    DatabusConsumer consumer = new DatabusConsumer();
    consumer.init(testStream, consumerName, null, config);
    Assert.assertEquals(consumer.getTopicName(), testStream);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);
    Map<PartitionId, PartitionReader> readers = consumer.getPartitionReaders();
    Assert.assertEquals(readers.size(), collectors.length);

    int i;
    for (i = 0; i < 20; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }
    consumer.mark(); 
    for (i = 20; i < 30; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();

    for (i = 20; i < 140; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }

    consumer.mark();
    Checkpoint lastCheckpoint = new Checkpoint(
        consumer.getCurrentCheckpoint().toBytes());

    for (i = 140; i < 160; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();
    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }

    consumer.close();

    // test checkpoint and consumer crash
    consumer = new DatabusConsumer();
    consumer.init(testStream, consumerName, null, config);
    Assert.assertEquals(consumer.getCurrentCheckpoint(), lastCheckpoint);

    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();

    consumer.close();

  }

  @Test
  public void testMarkAndResetWithStartTime() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck2);
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
          MessageUtil.constructMessage(i));
    }
    consumer.mark(); 
    for (i = 120; i < 130; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();

    for (i = 120; i < 240; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }

    consumer.mark();
    Checkpoint lastCheckpoint = new Checkpoint(
        consumer.getCurrentCheckpoint().toBytes());

    for (i = 240; i < 260; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();
    for (i = 240; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }

    consumer.close();

    // test checkpoint and consumer crash
    consumer = new DatabusConsumer();
    consumer.init(testStream, consumerName, null, config);
    Assert.assertEquals(consumer.getCurrentCheckpoint(), lastCheckpoint);

    for (i = 240; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();

    consumer.close();
  }

  @Test
  public void testMultipleClusters() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusClustersConfig,
        "testcluster1,testcluster2");
    config.set(DatabusConsumerConfig.checkpointDirConfig,
        ck3);
    assertMessages(config, 2, 1);
  }

  @Test
  public void testMultipleClusters2() throws Exception {
    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusClustersConfig,
        "testcluster1,testcluster2,testcluster3");
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck4);
    assertMessages(config, 3, 1);
  }

  @Test
  public void testMultipleClusters3() throws Exception {

    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.databusClustersConfig,
        null);
    config.set(DatabusConsumerConfig.checkpointDirConfig, ck5);
    assertMessages( config, 3, 1);
  }


  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }

}
