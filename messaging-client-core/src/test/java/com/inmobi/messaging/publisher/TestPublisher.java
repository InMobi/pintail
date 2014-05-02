package com.inmobi.messaging.publisher;

/*
 * #%L
 * messaging-client-core
 * %%
 * Copyright (C) 2012 - 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.EndOfStreamException;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.MockInMemoryConsumer;
import com.inmobi.messaging.stats.MockStatsEmitter;
import com.inmobi.messaging.util.AuditUtil;
import com.inmobi.stats.emitter.EmitMondemand;

public class TestPublisher {

  @Test
  public void test() throws IOException, TException {
    ClientConfig conf = new ClientConfig();
    conf.set(MessagePublisherFactory.PUBLISHER_CLASS_NAME_KEY,
        MockPublisher.class.getName());
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(conf);
    doTest(publisher);
    Assert.assertFalse(publisher.getMetrics().statEmissionEnabled());
    Assert.assertNull((publisher.getMetrics().getStatsEmitter()));

  }

  @Test
  public void testLoadFromClasspath() throws IOException {
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create();
    doTest(publisher);
    Assert.assertTrue(publisher.getMetrics().statEmissionEnabled());
    Assert.assertTrue(((MockStatsEmitter) publisher.getMetrics()
        .getStatsEmitter()).inited);
  }

  @Test
  public void testLoadFromFileName() throws IOException {
    URL url =
        getClass().getClassLoader().getResource(
            MessagePublisherFactory.MESSAGE_CLIENT_CONF_FILE);
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory
            .create(url.getFile());
    doTest(publisher);
    Assert.assertTrue(publisher.getMetrics().statEmissionEnabled());
    Assert.assertTrue(((MockStatsEmitter) publisher.getMetrics()
        .getStatsEmitter()).inited);
  }

  @Test
  public void testLoadFromClassName() throws IOException {
    ClientConfig conf = new ClientConfig();
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(conf,
            MockPublisher.class.getName());
    doTest(publisher);
    Assert.assertFalse(publisher.getMetrics().statEmissionEnabled());
    Assert.assertNull((publisher.getMetrics().getStatsEmitter()));
  }

  @Test
  public void testMondemand() throws IOException {
    ClientConfig conf = new ClientConfig();
    URL url =
        getClass().getClassLoader().getResource("mondemand-emitter.properties");
    conf.set(MessagePublisherFactory.EMITTER_CONF_FILE_KEY, url.getFile());
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(conf,
            MockPublisher.class.getName());
    doTest(publisher);
    Assert.assertTrue(publisher.getMetrics().statEmissionEnabled());
    Assert.assertTrue(
        (publisher.getMetrics().getStatsEmitter()) instanceof EmitMondemand);
  }

  @Test
  public void testPublisherWithHeaders() throws IOException,
      InterruptedException, EndOfStreamException {
    ClientConfig conf = new ClientConfig();
    conf.set("publisher.classname",
        "com.inmobi.messaging.publisher.MockInMemoryPublisher");
    conf.set(AuditService.WINDOW_SIZE_KEY, "60");
    conf.set(AuditService.AGGREGATE_WINDOW_KEY, "60");
    MessagePublisher publisher =
        MessagePublisherFactory.create(conf,
            MockInMemoryPublisher.class.getName());
    publisher.publish("topic", new Message("message".getBytes()));
    publisher.close();
    conf.set("topic.name", "topic");
    conf.set("consumer.name", "c1");
    MessageConsumer consumer =
        MessageConsumerFactory.create(conf,
            MockInMemoryConsumer.class.getName());
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    Message m = consumer.next();
    String msg = new String(m.getData().array());
    assert (msg.equals("message"));

  }

  @Test
  public void testAuditMessage() throws IOException, InterruptedException,
      TException, EndOfStreamException {
    ClientConfig conf = new ClientConfig();
    conf.set("publisher.classname",
        "com.inmobi.messaging.publisher.MockInMemoryPublisher");
    conf.set(AuditService.WINDOW_SIZE_KEY, "60");
    conf.set(AuditService.AGGREGATE_WINDOW_KEY, "5");
    conf.set(AbstractMessagePublisher.AUDIT_ENABLED_KEY, "true");

    MessagePublisher publisher =
        MessagePublisherFactory.create(conf,
            MockInMemoryPublisher.class.getName());
    publisher.publish("topic", new Message("message".getBytes()));
    publisher.close();
    conf.set("topic.name", AuditUtil.AUDIT_STREAM_TOPIC_NAME);
    conf.set("consumer.name", "c1");
    MessageConsumer consumer =
        MessageConsumerFactory.create(conf,
            MockInMemoryConsumer.class.getName());
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    Message m = consumer.next();
    TDeserializer deserializer = new TDeserializer();
    AuditMessage audit = new AuditMessage();
    deserializer.deserialize(audit, m.getData().array());
    Collection<Long> values = audit.getReceived().values();
    assert (values.iterator().hasNext());
    assert (values.iterator().next() == 1);

  }

  @Test
  public void testAuditDisbaled() throws IOException {
    ClientConfig conf = new ClientConfig();
    conf.set("publisher.classname",
        "com.inmobi.messaging.publisher.MockInMemoryPublisher");
    conf.set(AuditService.WINDOW_SIZE_KEY, "60");
    conf.set(AuditService.AGGREGATE_WINDOW_KEY, "60");
    conf.set(AbstractMessagePublisher.AUDIT_ENABLED_KEY, "false");

    MessagePublisher publisher =
        MessagePublisherFactory.create(conf,
            MockInMemoryPublisher.class.getName());
    publisher.publish("topic", new Message("message".getBytes()));
    publisher.close();
    assert (!((MockInMemoryPublisher) publisher).source
        .containsKey(AuditUtil.AUDIT_STREAM_TOPIC_NAME));

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
    Message msg = new Message(ByteBuffer.wrap(new byte[5]));

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
    Assert.assertEquals(publisher.getStats(topic).getInvocationCount(), 1,
        "invocation count");
    Assert.assertEquals(publisher.getStats(topic).getSuccessCount(), 1,
        "success count");
    Assert.assertEquals(publisher.getStats(topic).getUnhandledExceptionCount(),
        0, "unhandledexception count");
    Assert.assertEquals(MockPublisher.getMsg(topic), msg);
    MockPublisher.reset(topic);
    Assert.assertEquals(
        publisher.getStatsExposer(topic).getContexts()
            .get(TopicStatsExposer.STATS_TYPE_CONTEXT_NAME),
        TopicStatsExposer.STATS_TYPE);
    Assert.assertEquals(
        publisher.getStatsExposer(topic).getContexts()
            .get(TopicStatsExposer.TOPIC_CONTEXT_NAME), topic);
  }

  @Test (expectedExceptions = {IllegalStateException.class })
  public void testPublishAfterClose() throws IOException {
    ClientConfig conf = new ClientConfig();
    conf.set(MessagePublisherFactory.PUBLISHER_CLASS_NAME_KEY,
        MockPublisher.class.getName());
    AbstractMessagePublisher publisher =
        (AbstractMessagePublisher) MessagePublisherFactory.create(conf);
    publisher.publish("sample-topic", new Message("msg".getBytes()));
    publisher.close();
    publisher.publish("sample-topic", new Message("messages".getBytes()));
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
    // intializing latch here to ensure all threads run in parallel,without this
    // testng executes the threads in sequential order`
    CountDownLatch startLatch = new CountDownLatch(10);
    Assert.assertNull(publisher.getStats(topic));
    PublishThread p1 = new PublishThread(startLatch, topic, publisher);
    PublishThread p2 = new PublishThread(startLatch, topic, publisher);
    p1.start();
    p2.start();
    p1.join();
    p2.join();
    Assert.assertEquals(publisher.getStats(topic).getInvocationCount(), 2,
        "invocation count");
    Assert.assertEquals(publisher.getStats(topic).getSuccessCount(), 2,
        "success count");
    Assert.assertEquals(publisher.getStats(topic).getUnhandledExceptionCount(),
        0, "unhandledexception count");
    Assert.assertEquals(
        publisher.getStatsExposer(topic).getContexts()
            .get(TopicStatsExposer.STATS_TYPE_CONTEXT_NAME),
        TopicStatsExposer.STATS_TYPE);
    Assert.assertEquals(
        publisher.getStatsExposer(topic).getContexts()
            .get(TopicStatsExposer.TOPIC_CONTEXT_NAME), topic);
    Assert.assertFalse(publisher.getMetrics().statEmissionEnabled());
    Assert.assertNull((publisher.getMetrics().getStatsEmitter()));
    publisher.close();
  }

  class PublishThread extends Thread {

    private String topic;
    private AbstractMessagePublisher publisher;
    private final CountDownLatch startLatch;

    PublishThread(CountDownLatch startLatch, String topic,
        AbstractMessagePublisher publisher) {
      this.startLatch = startLatch;
      this.topic = topic;
      this.publisher = publisher;
    }

    public void run() {
      Message msg = new Message(ByteBuffer.wrap(new byte[5]));
      publisher.publish(topic, msg);
      Assert.assertEquals(MockPublisher.getMsg(topic), msg);
    }
  }
}
