package com.inmobi.messaging.consumer;

import java.net.URL;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class TestConsumer {

  @Test
  public void test() {
    ClientConfig conf = new ClientConfig();
    conf.set(MessageConsumerFactory.CONSUMER_CLASS_NAME_KEY,
        MockConsumer.class.getName());
    conf.set(MessageConsumerFactory.TOPIC_NAME_KEY, "test");
    conf.set(MessageConsumerFactory.CONSUMER_NAME_KEY, "testconsumer");
    AbstractMessageConsumer consumer =
        (AbstractMessageConsumer) MessageConsumerFactory.create(conf);
    doTest(consumer);
  }

  @Test
  public void testLoadFromClasspath() {
    AbstractMessageConsumer consumer =
        (AbstractMessageConsumer) MessageConsumerFactory.create();
    doTest(consumer);
  }

  @Test
  public void testLoadFromFileName() {
    URL url = getClass().getClassLoader().getResource(
        MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
    AbstractMessageConsumer consumer =
        (AbstractMessageConsumer) MessageConsumerFactory.create(
            url.getFile());
    doTest(consumer);
  }

  @Test
  public void testLoadFromClassName() {
    ClientConfig conf = new ClientConfig();
    conf.set(MessageConsumerFactory.TOPIC_NAME_KEY, "test");
    conf.set(MessageConsumerFactory.CONSUMER_NAME_KEY, "testconsumer");
    AbstractMessageConsumer consumer = 
      (AbstractMessageConsumer) MessageConsumerFactory.create(
          conf, MockConsumer.class.getName());
    doTest(consumer);
  }

  @Test
  public void testTopicNConsumerName() {
    AbstractMessageConsumer consumer = 
      (AbstractMessageConsumer) MessageConsumerFactory.create(
          new ClientConfig(), MockConsumer.class.getName(), "test",
          "testconsumer");
    doTest(consumer);
  }

  private void doTest(AbstractMessageConsumer consumer) {
    Assert.assertTrue(consumer instanceof MockConsumer);
    Assert.assertFalse(consumer.isMarkSupported());
    Assert.assertTrue(((MockConsumer)consumer).initedConf);
    Assert.assertEquals(consumer.getTopicName(), "test");
    Assert.assertEquals(consumer.getConsumerName(), "testconsumer");
    
    Message msg = consumer.next();
    consumer.close();
    Assert.assertEquals(new String(msg.getData().array()), MockConsumer.mockMsg);
  }
  
}
