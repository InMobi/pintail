package com.inmobi.messaging.consumer;

import java.io.IOException;
import java.net.URL;
import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.stats.MockStatsEmitter;
import com.inmobi.stats.emitter.EmitMondemand;

public class TestConsumer {
  private Date now = new Date(System.currentTimeMillis());

  @Test
  public void test() throws Exception {
    ClientConfig conf = new ClientConfig();
    conf.set(MessageConsumerFactory.CONSUMER_CLASS_NAME_KEY,
        MockConsumer.class.getName());
    conf.set(MessageConsumerFactory.TOPIC_NAME_KEY, "test");
    conf.set(MessageConsumerFactory.CONSUMER_NAME_KEY, "testconsumer");
    AbstractMessageConsumer consumer =
        (AbstractMessageConsumer) MessageConsumerFactory.create(conf);
    doTest(consumer, null, false, false);
  }

  @Test
  public void testLoadFromClasspath() throws Exception {
    AbstractMessageConsumer consumer =
        (AbstractMessageConsumer) MessageConsumerFactory.create();
    doTest(consumer, null, true, false);
  }

  @Test
  public void testLoadFromFileName() throws Exception {
    URL url = getClass().getClassLoader().getResource(
        MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
    AbstractMessageConsumer consumer =
        (AbstractMessageConsumer) MessageConsumerFactory.create(
            url.getFile());
    doTest(consumer, null, true, false);
  }

  @Test
  public void testLoadFromClassName() throws Exception {
    ClientConfig conf = new ClientConfig();
    conf.set(MessageConsumerFactory.TOPIC_NAME_KEY, "test");
    conf.set(MessageConsumerFactory.CONSUMER_NAME_KEY, "testconsumer");
    AbstractMessageConsumer consumer = 
      (AbstractMessageConsumer) MessageConsumerFactory.create(
          conf, MockConsumer.class.getName());
    doTest(consumer, null, false, false);
  }

  @Test
  public void testTopicNConsumerName() throws Exception {
    AbstractMessageConsumer consumer = 
      (AbstractMessageConsumer) MessageConsumerFactory.create(
          new ClientConfig(), MockConsumer.class.getName(), "test",
          "testconsumer");
    doTest(consumer, null, false, false);
  }

  @Test
  public void testWithStartTime() throws Exception {
    ClientConfig conf = new ClientConfig();
    conf.set(MessageConsumerFactory.CONSUMER_CLASS_NAME_KEY,
        MockConsumer.class.getName());
    conf.set(MessageConsumerFactory.TOPIC_NAME_KEY, "test");
    conf.set(MessageConsumerFactory.CONSUMER_NAME_KEY, "testconsumer");
    AbstractMessageConsumer consumer =
        (AbstractMessageConsumer) MessageConsumerFactory.create(conf, now);
    doTest(consumer, now, false, false);
  }

  @Test
  public void testLoadFromClasspathWithStartTime() throws Exception {
    AbstractMessageConsumer consumer =
        (AbstractMessageConsumer) MessageConsumerFactory.create(now);
    doTest(consumer, now, true, false);
  }

  @Test
  public void testLoadFromFileNameWithStartTime() throws Exception {
    URL url = getClass().getClassLoader().getResource(
        MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
    AbstractMessageConsumer consumer =
        (AbstractMessageConsumer) MessageConsumerFactory.create(
            url.getFile(), now);
    doTest(consumer, now, true, false);
  }

  @Test
  public void testLoadFromClassNameWithStartTime() throws Exception {
    ClientConfig conf = new ClientConfig();
    conf.set(MessageConsumerFactory.TOPIC_NAME_KEY, "test");
    conf.set(MessageConsumerFactory.CONSUMER_NAME_KEY, "testconsumer");
    AbstractMessageConsumer consumer = 
      (AbstractMessageConsumer) MessageConsumerFactory.create(
          conf, MockConsumer.class.getName(), now);
    doTest(consumer, now, false, false);
  }

  @Test
  public void testTopicNConsumerNameWithStartTime() throws Exception {
    AbstractMessageConsumer consumer = 
      (AbstractMessageConsumer) MessageConsumerFactory.create(
          new ClientConfig(), MockConsumer.class.getName(), "test",
          "testconsumer", now);
    doTest(consumer, now, false, false);
  }

  @Test
  public void testMondemand() throws Exception {
    ClientConfig conf = new ClientConfig();
    URL url = getClass().getClassLoader().getResource(
        "mondemand-emitter.properties");
    conf.set(MessageConsumerFactory.EMITTER_CONF_FILE_KEY, url.getFile());
    AbstractMessageConsumer consumer = 
        (AbstractMessageConsumer) MessageConsumerFactory.create(
            conf, MockConsumer.class.getName(), "test",
            "testconsumer", now);
    doTest(consumer, now, true, true);
  }

  @Test
  public void testStartTime()
      throws IOException, InterruptedException, EndOfStreamException {
    AbstractMessageConsumer consumer = 
      (AbstractMessageConsumer) MessageConsumerFactory.create(
          new ClientConfig(), MockConsumer.class.getName(), "test",
          "testconsumer", now);
    doTest(consumer, now, false, false);
  }

  private void doTest(AbstractMessageConsumer consumer, Date startTime,
      boolean statsEnabled, boolean isMondemand)
          throws InterruptedException, EndOfStreamException {
    Assert.assertTrue(consumer instanceof MockConsumer);
    Assert.assertFalse(consumer.isMarkSupported());
    Assert.assertTrue(((MockConsumer)consumer).initedConf);
    Assert.assertEquals(consumer.getTopicName(), "test");
    Assert.assertEquals(consumer.getConsumerName(), "testconsumer");
    Assert.assertEquals(consumer.getStartTime(), startTime);
    
    Message msg = consumer.next();
    consumer.close();
    Assert.assertEquals(new String(msg.getData().array()), MockConsumer.mockMsg);
    
    if (statsEnabled) {
      Assert.assertTrue(consumer.getStatsBuilder().statEmissionEnabled());
      if (isMondemand) {
        Assert.assertTrue((consumer.getStatsBuilder().getStatsEmitter())
            instanceof EmitMondemand);     
      } else {
        Assert.assertTrue(((MockStatsEmitter)consumer.getStatsBuilder()
            .getStatsEmitter()).inited);
      }
    } else {
      Assert.assertFalse(consumer.getStatsBuilder().statEmissionEnabled());
      Assert.assertNull((consumer.getStatsBuilder().getStatsEmitter()));
    }
    Assert.assertEquals(((BaseMessageConsumerStatsExposer)consumer.getMetrics())
        .getNumMessagesConsumed(), 1);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer)consumer.getMetrics())
        .getContexts().size(), 2);
    Assert.assertEquals(((BaseMessageConsumerStatsExposer)consumer.getMetrics())
        .getContexts().get(BaseMessageConsumerStatsExposer.TOPIC_CONTEXT),
        consumer.getTopicName());
    Assert.assertEquals(((BaseMessageConsumerStatsExposer)consumer.getMetrics())
        .getContexts().get(BaseMessageConsumerStatsExposer.CONSUMER_CONTEXT),
        consumer.getConsumerName());
  }  
}
