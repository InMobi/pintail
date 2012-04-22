package com.inmobi.messaging.consumer;


public class MessageConsumerFactory {

  public static final String DEFAULT_MSG_CONSUMER =
      "com.inmobi.databus.consumer.DatabusConsumer.class";

  public static MessageConsumer create(String consumerName, String streamName)
      throws Exception {
    return create(consumerName, streamName, DEFAULT_MSG_CONSUMER);
  }

  public static MessageConsumer create(String consumerName, String streamName,
      String consumerClass) throws Exception {
    AbstractMessageConsumer consumer = (AbstractMessageConsumer) Class.forName(
        consumerClass).newInstance();
    consumer.init(consumerName, streamName);
    return consumer;
  }
}
