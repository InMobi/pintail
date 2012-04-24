package com.inmobi.messaging;

import java.net.URL;
import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPublisher {

  @Test
  public void test() {
    ClientConfig conf = new ClientConfig();
    conf.set(MessagePublisherFactory.PUBLISHER_CLASS_NAME_KEY,
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
        MessagePublisherFactory.MESSAGE_CLIENT_CONF_FILE);
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(
            url.getFile());
    doTest(publisher);
    Assert.assertTrue(publisher.statEmissionEnabled());
    Assert.assertTrue(MockStatsEmitter.inited);
  }

  private void doTest(AbstractMessagePublisher publisher) {
    Message msg = new Message("test", ByteBuffer.wrap(new byte[5]));
    long invocation = publisher.getStats().getInvocationCount();
    long success = publisher.getStats().getSuccessCount();
    long unhandledException = publisher.getStats().getUnhandledExceptionCount();
    publisher.publish(msg);
    Assert.assertEquals(publisher.getStats().getInvocationCount(), 
        invocation + 1, "invocation count");
    Assert.assertEquals(publisher.getStats().getSuccessCount(), 
        success + 1, "success count");
    Assert.assertEquals(publisher.getStats().getUnhandledExceptionCount(), 
        unhandledException, "unhandledexception count");
    publisher.close();
    Assert.assertEquals(MockPublisher.msg, msg);
    MockPublisher.reset();
  }

  
}
