package com.inmobi.messaging.logger;

/*
 * #%L
 * messaging-client-logappender
 * %%
 * Copyright (C) 2014 InMobi
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

import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.testng.annotations.Test;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.MockPublisher;
import com.inmobi.messaging.util.AuditUtil;

public class TestLogger {

  @Test
  public void test() throws TException {
    PropertyConfigurator.configure("src/test/resources/log4j.properties");
    Logger logger = Logger.getLogger("messagingclient");
    MessageAppender appender =
        (MessageAppender) logger.getAppender("messagingclient");
    Assert.assertNull(appender.getConffile());
    doTest(logger, appender);
  }

  @Test
  public void testWithConf() throws TException {
    PropertyConfigurator
        .configure("src/test/resources/log4j-with-conf.properties");
    Logger logger = Logger.getLogger("messagingclient");
    MessageAppender appender =
        (MessageAppender) logger.getAppender("messagingclient");
    Assert.assertEquals(appender.getConffile(),
        "src/test/resources/messaging-publisher-conf.properties");
    doTest(logger, appender);
  }

  private void doTest(Logger logger, MessageAppender appender)
      throws TException {
    String topic = "test";
    Assert.assertEquals(appender.getTopic(), topic);
    Message msg = new Message(ByteBuffer.wrap("hello".getBytes()));
    logger.info(msg);
    Assert.assertEquals(MockPublisher.getMsg(topic), msg);
    MockPublisher.reset(topic);

    // test byte[] logging
    msg = new Message(ByteBuffer.wrap("hello".getBytes()));
    logger.info(msg.getData().array());
    ByteBuffer returned =
        AuditUtil.removeHeader(MockPublisher.getMsg(topic).getData().array());
    Assert.assertEquals(new Message(returned), msg);
    MockPublisher.reset(topic);

    // test String logging
    msg = new Message(ByteBuffer.wrap("hello".getBytes()));
    logger.info(new String(msg.getData().array()));
    returned =
        AuditUtil.removeHeader(MockPublisher.getMsg(topic).getData().array());
    Assert.assertEquals(new Message(returned), msg);
    MockPublisher.reset(topic);

    // test Object logging. any other kind must be ignored
    logger.info(new Object());
    Assert.assertNull(MockPublisher.getMsg(topic));
    MockPublisher.reset(topic);

    // test thrift object logging
    LogEntry le = new LogEntry();
    le.category = "xxxx";
    le.message = "massage";
    logger.info(le);
    returned =
        AuditUtil.removeHeader(MockPublisher.getMsg(topic).getData().array());
    TSerializer serializer = new TSerializer();
    Assert.assertEquals(new Message(returned),
        new Message(serializer.serialize(le)));
    MockPublisher.reset(topic);
  }
}
