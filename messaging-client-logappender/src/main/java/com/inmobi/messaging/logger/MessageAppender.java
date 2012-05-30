package com.inmobi.messaging.logger;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

/*
 * Setting of fixed topic is deprecated.
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

  @Deprecated
  public String getTopic() {
    return topic;
  }

  /*
   * Setting of fixed topic is deprecated
   */
  @Deprecated
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
    else if (topic != null) {
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
    }
    
    if (msg != null) {
      publisher.publish(topic, msg);
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
    } catch(IOException e) {
      throw new RuntimeException("Could not create publisher", e);
    }
    
  }
}
