package com.inmobi.messaging.consumer;

import java.io.InputStream;

import com.inmobi.messaging.ClientConfig;

public class MessageConsumerFactory {

  public static final String MESSAGE_CLIENT_CONF_FILE = "messaging-consumer-conf.properties";
  public static final String CONSUMER_CLASS_NAME_KEY = "consumer.classname";

  /*
   * Creates MessageConsumer by loading the properties
   * config file from classpath.
   */
  public static MessageConsumer create() {
    InputStream in = ClientConfig.class.getClassLoader().getResourceAsStream(
        MESSAGE_CLIENT_CONF_FILE);
    if (in == null) {
      throw new RuntimeException("could not load conf file "
          + MESSAGE_CLIENT_CONF_FILE + " from classpath.");
    }
    ClientConfig config = ClientConfig.load(in);
    return create(config);
  }

  /*
   * Creates MessageConsumer by loading the passed config file.
   */
  public static MessageConsumer create(String confFile) {
    ClientConfig config = ClientConfig.load(confFile);
    return create(config);
  }

  /*
   * Creates MessageConsumer using the passed config file.
   */
  public static MessageConsumer create(ClientConfig config) {
    Class<?> clazz;
    String consumerName = config.getString(CONSUMER_CLASS_NAME_KEY);
    AbstractMessageConsumer consumer = null;
    try {
      clazz = Class.forName(consumerName);
      consumer = (AbstractMessageConsumer) clazz.newInstance();

    } catch (Exception e) {
      throw new RuntimeException("Could not create message consumer "
          + config.getString(CONSUMER_CLASS_NAME_KEY), e);
    }
    consumer.init(config);
    return consumer;
  }
}
