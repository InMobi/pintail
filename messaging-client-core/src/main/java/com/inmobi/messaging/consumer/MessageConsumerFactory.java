package com.inmobi.messaging.consumer;

import java.io.InputStream;

import com.inmobi.messaging.ClientConfig;

/**
 * The factory which creates a concrete implementation of 
 * {@link MessageConsumer} 
 *
 */
public class MessageConsumerFactory {

  public static final String MESSAGE_CLIENT_CONF_FILE = 
                          "messaging-consumer-conf.properties";
  public static final String CONSUMER_CLASS_NAME_KEY = "consumer.classname";
  public static final String TOPIC_NAME_KEY = "topic.name";
  public static final String CONSUMER_NAME_KEY = "consumer.name";

  /**
   * Creates concrete class extending {@link AbstractMessageConsumer} given by
   * name {@value #CONSUMER_CLASS_NAME_KEY}, by loading the properties from
   * configuration file named {@value #MESSAGE_CLIENT_CONF_FILE} from classpath.
   * 
   * Initializes the consumer class with passed configuration, topicName with
   * the value of {@value #TOPIC_NAME_KEY}, consumerName with the value of 
   * {@value #CONSUMER_NAME_KEY}.
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
   * Creates concrete class extending {@link AbstractMessageConsumer} given by
   * name {@value #CONSUMER_CLASS_NAME_KEY}, by loading the passed
   * configuration file.
   * 
   * Initializes the consumer class with passed configuration, topicName with
   * the value of {@value #TOPIC_NAME_KEY}, consumerName with the value of 
   * {@value #CONSUMER_NAME_KEY}.
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
   * Creates concrete class extending {@link AbstractMessageConsumer} given by
   * name {@value #CONSUMER_CLASS_NAME_KEY}, using the passed configuration
   * object.
   * 
   * Initializes the consumer class with passed configuration, topicName with
   * the value of {@value #TOPIC_NAME_KEY}, consumerName with the value of 
   * {@value #CONSUMER_NAME_KEY}.
   *
   * @param config {@link ClientConfig} object
   * @return {@link MessageConsumer} concrete object
   */
  public static MessageConsumer create(ClientConfig config) {
    String consumerName = config.getString(CONSUMER_CLASS_NAME_KEY);
    return create(config, consumerName);
  }
  
  /**
   * Creates concrete class extending {@link AbstractMessageConsumer} with
   * passed class name and using the passed configuration object.
   * 
   * Initializes the consumer class with passed configuration, topicName with
   * the value of {@value #TOPIC_NAME_KEY}, consumerName with the value of 
   * {@value #CONSUMER_NAME_KEY}.
   *
   * @param config {@link ClientConfig} object
   * @param consumerClassName The class name of the consumer implementation
   * 
   * @return {@link MessageConsumer} concrete object
   */
  public static MessageConsumer create(ClientConfig config,
                                       String consumerClassName) {
    return create(config, consumerClassName, config.getString(TOPIC_NAME_KEY),
        config.getString(CONSUMER_NAME_KEY));
  }
  
  private static AbstractMessageConsumer createAbstractConsumer(
      String consumerClassName) {
    Class<?> clazz;
    AbstractMessageConsumer consumer = null;
    try {
      clazz = Class.forName(consumerClassName);
      consumer = (AbstractMessageConsumer) clazz.newInstance();

    } catch (Exception e) {
      throw new RuntimeException("Could not create message consumer "
          + consumerClassName, e);
    }
    return consumer;
  }
  
  /**
   * Creates concrete class extending {@link AbstractMessageConsumer} with
   * passed name and using the passed configuration object. Also initializes
   * the consumer class with passed configuration object, streamName and
   * consumerName.
   *
   * @param config {@link ClientConfig} object
   * @param consumerClassName The class name of the consumer implementation
   * @param topicName The name of the topic being consumed
   * @param consumerName The name of the conusmer being consumed
   * 
   * @return {@link MessageConsumer} concrete object
   */
  public static MessageConsumer create(ClientConfig config,
                                       String consumerClassName,
                                       String topicName,
                                       String consumerName) {
    AbstractMessageConsumer consumer = createAbstractConsumer(consumerClassName);
    consumer.init(topicName, consumerName, config);
    return consumer;
  }

}
