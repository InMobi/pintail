package com.inmobi.messaging.consumer;


public abstract class AbstractMessageConsumer implements MessageConsumer {

  private String streamName;
  private String consumerName;

  protected void init(String consumerName, String streamName) {
    this.consumerName = consumerName;
    this.streamName = streamName;
   start();
  }

  protected abstract void start();

  public String getStreamName() {
    return this.streamName;
  }

  public String getConsumerName() {
    return this.consumerName;
  }
}
