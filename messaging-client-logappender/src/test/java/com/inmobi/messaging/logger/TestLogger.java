package com.inmobi.messaging.logger;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.testng.annotations.Test;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.MockPublisher;

public class TestLogger {

  @Test
  public void test() throws TException {
    PropertyConfigurator.configure("src/test/resources/log4j.properties");
    Logger logger = Logger.getLogger("messagingclient");
    MessageAppender appender = (MessageAppender) logger
        .getAppender("messagingclient");
    Assert.assertNull(appender.getConffile());
    doTest(logger, appender);
  }

  @Test
  public void testWithConf() throws TException {
    PropertyConfigurator
        .configure("src/test/resources/log4j-with-conf.properties");
    Logger logger = Logger.getLogger("messagingclient");
    MessageAppender appender = (MessageAppender) logger
        .getAppender("messagingclient");
    Assert.assertEquals(appender.getConffile(),
        "src/test/resources/messaging-client-conf.properties");
    doTest(logger, appender);
  }

  private void doTest(Logger logger, MessageAppender appender)
      throws TException {
    String topic = "test";
    Assert.assertEquals(appender.getTopic(), topic);
    Message msg = new Message(topic, "hello".getBytes());
    logger.info(msg);
    Assert.assertEquals(MockPublisher.msg, msg);
    MockPublisher.reset();

    // test byte[] logging
    logger.info(msg.getMessage());
    Assert.assertEquals(MockPublisher.msg, msg);
    MockPublisher.reset();

    // test String logging
    logger.info(new String(msg.getMessage()));
    Assert.assertEquals(MockPublisher.msg, msg);
    MockPublisher.reset();

    // test Object logging. any other kind must be ignored
    logger.info(new Object());
    Assert.assertNull(MockPublisher.msg);
    MockPublisher.reset();

    // test thrift object logging
    LogEntry le = new LogEntry();
    le.category = "xxxx";
    le.message = "massage";
    logger.info(le);
    TSerializer serializer = new TSerializer();
    Assert.assertEquals(MockPublisher.msg, 
        new Message(topic, serializer.serialize(le)));
    MockPublisher.reset();
  }
}
