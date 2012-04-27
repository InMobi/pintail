package com.inmobi.messaging.consumer;

import java.io.InputStream;

import com.inmobi.messaging.ClientConfig;

/**
 * The factory which creates a concrete implementation of 
 * {@link MessageConsumer} 
 *
 */
public class MessageConsumerFactory {

  public static final String MESSAGE_CLIENT_CONF_FILE = "messaging-consumer-conf.properties";
  public static final String CONSUMER_CLASS_NAME_KEY = "consumer.classname";

  /**
   * Creates concrete class of {@link AbstractMessageConsumer} given by name
   * {@value #CONSUMER_CLASS_NAME_KEY}, by loading the properties from
   * configuration file named {@value #MESSAGE_CLIENT_CONF_FILE} from classpath.
   * 
   * @return {@link MessageConsumer} concrete object
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

  /**
   * Creates concrete class of {@link AbstractMessageConsumer} given by name
   * {@value #CONSUMER_CLASS_NAME_KEY}, by loading the passed
   * configuration file.
   *
   * @param confFile The file name
   *  
   * @return {@link MessageConsumer} concrete object
   */
  public static MessageConsumer create(String confFile) {
    ClientConfig config = ClientConfig.load(confFile);
    return create(config);
  }

  /**
   * Creates concrete class of {@link AbstractMessageConsumer} given by name
   * {@value #CONSUMER_CLASS_NAME_KEY}, using the passed configuration
   * object.
   *
   * @param config {@link ClientConfig} object
   * @return {@link MessageConsumer} concrete object
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
