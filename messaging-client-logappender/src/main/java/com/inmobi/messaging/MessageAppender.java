package com.inmobi.messaging;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class MessageAppender extends AppenderSkeleton {

  private String topic;

  private String publisherClass;
  private MessagePublisher publisher;

  public String getPublisherClass() {
    return publisherClass;
  }

  public void setPublisherClass(String publisherClass) {
    this.publisherClass = publisherClass;
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
    if (o instanceof byte[]) {
      publisher.publish(new Message((byte[]) o));
    } else if (o instanceof String) {
      publisher.publish(new Message(((String) o).getBytes()));
    } /*else 
      //TBase support only for backward compatibility
      //would be deprecated
      if (o instanceof TBase) {
        TBase thriftObject = (TBase) o;
        TNettyChannelBuffer t = new TNettyChannelBuffer(null, 
            ChannelBuffers.dynamicBuffer());
        TProtocol p = new TBinaryProtocol(t);
        thriftObject.write(p);
        publisher.publish(thriftObject);
      }*/
  }

  @Override
  public void activateOptions() {
    super.activateOptions();
    try {
      Class clz = Class.forName(publisherClass);
      publisher = (AbstractMessagePublisher) clz.newInstance();
      ClientConfig config = ClientConfig.load();
      publisher.init(topic, config);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
