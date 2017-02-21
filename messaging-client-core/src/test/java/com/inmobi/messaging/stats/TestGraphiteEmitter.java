package com.inmobi.messaging.stats;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.publisher.MockInMemoryPublisher;

import com.inmobi.messaging.PintailException;
import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Properties;

public class TestGraphiteEmitter {

  @Test
  public void test() throws IOException, TException, InterruptedException, URISyntaxException {
    URL resource = getClass().getClassLoader().getResource("graphite-statemitter.properties");
    ClientConfig conf = new ClientConfig();
    conf.set(MessagePublisherFactory.PUBLISHER_CLASS_NAME_KEY,
             MockInMemoryPublisher.class.getName());
    conf.set(MessagePublisherFactory.EMITTER_CONF_FILE_KEY,
             resource.toURI().getPath());
    Properties graphiteEmitterProperties = new Properties();
    graphiteEmitterProperties.load(new FileInputStream(new File(resource.toURI())));
    MockGraphiteServer
      mockGraphiteServer =
      new MockGraphiteServer(Integer.parseInt(graphiteEmitterProperties.getProperty("graphite.emitter.port")));
    mockGraphiteServer.start();
    MockInMemoryPublisher messagePublisher =
      (MockInMemoryPublisher) MessagePublisherFactory.create(conf);

    try {
      messagePublisher.publish("Topic1", new Message(ByteBuffer.wrap("This is the first message".getBytes())));
      messagePublisher.publish("Topic2", new Message(ByteBuffer.wrap("This is the failure message".getBytes())));
    } catch (PintailException e) {
      e.printStackTrace();
    }
    messagePublisher.incrementSuccessCount("Topic1");
    messagePublisher.incrementFailedCount("Topic2");
    messagePublisher.close();

    mockGraphiteServer.stop();

    Assert.assertEquals(mockGraphiteServer.getMetricMap().get("Topic1.successCount"), "1");
    Assert.assertEquals(mockGraphiteServer.getMetricMap().get("Topic2.unhandledExceptionCount"), "1");

  }
}
