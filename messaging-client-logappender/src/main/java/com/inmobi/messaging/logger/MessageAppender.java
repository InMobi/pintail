package com.inmobi.messaging.logger;

/*
 * #%L
 * messaging-client-logappender
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
import java.nio.ByteBuffer;

import com.inmobi.messaging.PintailException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

/**
 * Only com.inmobi.messaging.Message is valid object type in
 * LoggingEvent.getMessage(). byte[], String and TBase types are deprecated.
 */
public class MessageAppender extends AppenderSkeleton {

  private final TSerializer serializer = new TSerializer();
  private String topic;

  private String conffile;
  private MessagePublisher publisher;

  public String getConffile() {
    return conffile;
  }

  public void setConffile(String conffile) {
    this.conffile = conffile;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  @Override
  public void close() {
    if (publisher != null) {
      publisher.close();
    }
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  protected void append(LoggingEvent event) {
    Object o = event.getMessage();
    Message msg = null;
    if (o instanceof Message) {
      msg = (Message) o;
    }

    //deprecated support only if fixed topic is set
    if (o instanceof byte[]) {
      msg = new Message(ByteBuffer.wrap((byte[]) o));
    } else if (o instanceof String) {
      msg = new Message(ByteBuffer.wrap(((String) o).getBytes()));
    } else if (o instanceof TBase) {
      TBase thriftOb = (TBase) o;
      try {
        msg = new Message(
            ByteBuffer.wrap(serializer.serialize(thriftOb)));
      } catch (TException e) {
        System.out.println("Could not serialize thrift object");
        e.printStackTrace();
      }
    }

    if (msg != null) {
      try {
        publisher.publish(topic, msg);
      } catch (PintailException e) {
        System.out.println("could not publish the message");
        e.printStackTrace();
      }
    }
  }

  @Override
  public void activateOptions() {
    super.activateOptions();
    System.out.println("Config file set is: " + conffile);
    System.out.println("Fixed topic set is: " + topic);
    try {
    if (conffile != null) {
      publisher = MessagePublisherFactory.create(conffile);
    } else {
      publisher = MessagePublisherFactory.create();
    }
    } catch (IOException e) {
      throw new RuntimeException("Could not create publisher", e);
    }

  }
}
