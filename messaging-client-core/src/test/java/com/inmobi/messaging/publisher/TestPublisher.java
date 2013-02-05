package com.inmobi.messaging.publisher;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.MockInMemoryConsumer;
import com.inmobi.messaging.stats.MockStatsEmitter;
import com.inmobi.stats.emitter.EmitMondemand;

public class TestPublisher {

  @Test
  public void test() throws IOException, TException {
    ClientConfig conf = new ClientConfig();
    conf.set(MessagePublisherFactory.PUBLISHER_CLASS_NAME_KEY,
        MockPublisher.class.getName());
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(conf);
    // AuditService.worker.publisher = null;
    doTest(publisher);
    Assert.assertFalse(publisher.getMetrics().statEmissionEnabled());
    Assert.assertNull((publisher.getMetrics().getStatsEmitter()));

  }

  @Test
  public void testLoadFromClasspath() throws IOException {
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    // AuditService.worker.publisher = null;
    doTest(publisher);
    Assert.assertTrue(publisher.getMetrics().statEmissionEnabled());
    Assert.assertTrue((
(MockStatsEmitter) publisher.getMetrics()
        .getStatsEmitter()).inited);
  }

  @Test
  public void testLoadFromFileName() throws IOException {
    URL url = getClass().getClassLoader().getResource(
        MessagePublisherFactory.MESSAGE_CLIENT_CONF_FILE);
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(
            url.getFile());
    // AuditService.worker.publisher = null;
    doTest(publisher);
    Assert.assertTrue(publisher.getMetrics().statEmissionEnabled());
    Assert.assertTrue((
        (MockStatsEmitter)publisher.getMetrics().getStatsEmitter()).inited);
  }

  @Test
  public void testLoadFromClassName() throws IOException {
    ClientConfig conf = new ClientConfig();
    AbstractMessagePublisher publisher = 
      (AbstractMessagePublisher) MessagePublisherFactory.create(
          conf, MockPublisher.class.getName());
    // AuditService.worker.publisher = null;
    doTest(publisher);
    Assert.assertFalse(publisher.getMetrics().statEmissionEnabled());
    Assert.assertNull((publisher.getMetrics().getStatsEmitter()));
  }

  @Test
  public void testMondemand() throws IOException {
    ClientConfig conf = new ClientConfig();
    URL url = getClass().getClassLoader().getResource(
        "mondemand-emitter.properties");
    conf.set(MessagePublisherFactory.EMITTER_CONF_FILE_KEY, url.getFile());
    AbstractMessagePublisher publisher = 
      (AbstractMessagePublisher) MessagePublisherFactory.create(
          conf, MockPublisher.class.getName());
    // AuditService.worker.publisher = null;
    doTest(publisher);
    Assert.assertTrue(publisher.getMetrics().statEmissionEnabled());
    Assert.assertTrue((
        publisher.getMetrics().getStatsEmitter()) instanceof EmitMondemand);    
  }

  @Test
  public void testPublisherWithHeaders() throws IOException,
      InterruptedException {
    ClientConfig conf = new ClientConfig();
    conf.set("publisher.classname",
        "com.inmobi.messaging.publisher.MockInMemoryPublisher");
    conf.set("window.size.sec", "60");
    conf.set("aggregate.window.sec", "60");
    MessagePublisher publisher = MessagePublisherFactory.create(conf,
        MockInMemoryPublisher.class.getName());
    // AuditService.worker.publisher=null;
    publisher.publish("topic", new Message("message".getBytes()));
    publisher.close();
    conf.set("topic.name", "topic");
    conf.set("consumer.name", "c1");
    MessageConsumer consumer = MessageConsumerFactory.create(conf,
        MockInMemoryConsumer.class.getName());
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    Message m = consumer.next();
    String msg = new String(m.getData().array());
    assert (msg.equals("message"));

  }

  @Test
  public void testAuditMessage() throws IOException, InterruptedException,
      TException {
    ClientConfig conf = new ClientConfig();
    conf.set("publisher.classname",
        "com.inmobi.messaging.publisher.MockInMemoryPublisher");
    conf.set("window.size.sec", "60");
    conf.set("aggregate.window.sec", "60");

    MessagePublisher publisher = MessagePublisherFactory.create(conf,
        MockInMemoryPublisher.class.getName());
    // AuditService.worker.publisher = null;
    publisher.publish("topic", new Message("message".getBytes()));
    publisher.close();
    conf.set("topic.name", "audit");
    conf.set("consumer.name", "c1");
    MessageConsumer consumer = MessageConsumerFactory.create(conf,
        MockInMemoryConsumer.class.getName());
    // AuditService.worker.publisher=null;
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    Message m = consumer.next();
    TDeserializer deserializer = new TDeserializer();
    AuditMessage audit = new AuditMessage();
    deserializer.deserialize(audit, m.getData().array());
    Collection<Long> values = audit.getSent().values();
    assert (values.iterator().hasNext());
    assert (values.iterator().next() == 1);

  }

  private void doTest(AbstractMessagePublisher publisher) {
    String topic1 = "test1";
    String topic2 = "test2";
    doTest(topic1, publisher);
    doTest(topic2, publisher);
    publisher.close();
  }

  private void doTest(String topic, AbstractMessagePublisher publisher) {
    Throwable th = null;
    String nullTopic = null;
    // publish null message
    try {
      publisher.publish(topic, null);
    } catch (Throwable t) {
      th = t;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
    Message msg = new Message( ByteBuffer.wrap(new byte[5]));

    th = null;
    // publish to null topic
    try {
      publisher.publish(nullTopic, msg);
    } catch (Throwable t) {
      th = t;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
    
    // publish the message
    Assert.assertNull(publisher.getStats(topic));
    publisher.publish(topic, msg);
    Assert.assertEquals(publisher.getStats(topic).getInvocationCount(),
        1, "invocation count");
    Assert.assertEquals(publisher.getStats(topic).getSuccessCount(),
        1, "success count");
    Assert.assertEquals(publisher.getStats(topic).getUnhandledExceptionCount(),
        0, "unhandledexception count");
    Assert.assertEquals(MockPublisher.getMsg(topic), msg);
    MockPublisher.reset(topic);
    Assert.assertEquals(publisher.getStatsExposer(topic).getContexts().get(
        TopicStatsExposer.STATS_TYPE_CONTEXT_NAME),
        TopicStatsExposer.STATS_TYPE);
    Assert.assertEquals(publisher.getStatsExposer(topic).getContexts().get(
        TopicStatsExposer.TOPIC_CONTEXT_NAME), topic);
  }
  
  @Test
  public void testMultiplePublisherThreads() throws IOException,
      InterruptedException {
    ClientConfig conf = new ClientConfig();
    conf.set(MessagePublisherFactory.PUBLISHER_CLASS_NAME_KEY,
        MockPublisher.class.getName());
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(conf);
    String topic = "test";
    Assert.assertNull(publisher.getStats(topic));
    PublishThread p1 = new PublishThread(topic, publisher);
    PublishThread p2 = new PublishThread(topic, publisher);
    p1.start();
    p2.start();
    p1.join();
    p2.join();
    Assert.assertEquals(publisher.getStats(topic).getInvocationCount(),
        2, "invocation count");
    Assert.assertEquals(publisher.getStats(topic).getSuccessCount(),
        2, "success count");
    Assert.assertEquals(publisher.getStats(topic).getUnhandledExceptionCount(),
        0, "unhandledexception count"); 
    Assert.assertEquals(publisher.getStatsExposer(topic).getContexts().get(
        TopicStatsExposer.STATS_TYPE_CONTEXT_NAME),
        TopicStatsExposer.STATS_TYPE);
    Assert.assertEquals(publisher.getStatsExposer(topic).getContexts().get(
        TopicStatsExposer.TOPIC_CONTEXT_NAME), topic);
    Assert.assertFalse(publisher.getMetrics().statEmissionEnabled());
    Assert.assertNull((publisher.getMetrics().getStatsEmitter()));
    publisher.close();    
  }

  class PublishThread extends Thread {
      
    private String topic;
    private AbstractMessagePublisher publisher;
    
    PublishThread(String topic,
        AbstractMessagePublisher publisher) {
      this.topic = topic;
      this.publisher = publisher;
    }
      
    public void run(){
      Message msg = new Message( ByteBuffer.wrap(new byte[5]));
      publisher.publish(topic, msg);
      Assert.assertEquals(MockPublisher.getMsg(topic), msg);
    }
  }
}
