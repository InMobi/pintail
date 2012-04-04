package com.inmobi.messaging;

import java.net.URL;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPublisher {

  @Test
  public void test() {
    ClientConfig conf = new ClientConfig();
    conf.set(ClientConfig.PUBLISHER_CLASS_NAME_KEY,
        MockPublisher.class.getName());
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(conf);
    doTest(publisher);
    Assert.assertFalse(publisher.statEmissionEnabled());
    Assert.assertFalse(MockStatsEmitter.inited);
  }

  @Test
  public void testLoadFromClasspath() {
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    doTest(publisher);
    Assert.assertTrue(publisher.statEmissionEnabled());
    Assert.assertTrue(MockStatsEmitter.inited);
  }

  @Test
  public void testLoadFromFileName() {
    URL url = getClass().getClassLoader().getResource(
        ClientConfig.MESSAGE_CLIENT_CONF_FILE);
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(
            url.getFile());
    doTest(publisher);
    Assert.assertTrue(publisher.statEmissionEnabled());
    Assert.assertTrue(MockStatsEmitter.inited);
  }

  private void doTest(MessagePublisher publisher) {
    Message msg = new Message("test", new byte[5]);
    publisher.publish(msg);
    publisher.close();
    Assert.assertEquals(MockPublisher.msg, msg);
    MockPublisher.reset();
  }

  
}
