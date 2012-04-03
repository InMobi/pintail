package com.inmobi.messaging;

import java.net.URL;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPublisher {

  @Test
  public void test() {
    ClientConfig conf = new ClientConfig();
    conf.set(ClientConfig.PUBLISHER_CLASS_NAME_KEY,
        MockPublisher.class.getName());
    MessagePublisher publisher = MessagePublisherFactory.create(conf);
    doTest(publisher);
  }

  @Test
  public void testLoadFromClasspath() {
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    doTest(publisher);
    //Assert.assertTrue(publisher.statEmissionEnabled());
  }

  @Test
  public void testLoadFromFileName() {
    URL url = getClass().getClassLoader().getResource(
        ClientConfig.MESSAGE_CLIENT_CONF_FILE);
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(
            url.getFile());
    doTest(publisher);
    //Assert.assertTrue(publisher.statEmissionEnabled());
  }

  private void doTest(MessagePublisher publisher) {
    Message msg = new Message("test", new byte[5]);
    publisher.publish(msg);
    publisher.close();
    Assert.assertEquals(MockPublisher.msg, msg);
    MockPublisher.reset();
  }

  public static class MockPublisher extends AbstractMessagePublisher {
    private static Message msg;
    static void reset() {
      msg = null;
    }
    @Override
    protected void publish(Map<String, String> headers, Message m) {
      msg = m;
    }

  }
}
