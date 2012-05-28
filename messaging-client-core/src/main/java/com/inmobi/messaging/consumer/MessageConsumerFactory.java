package com.inmobi.messaging.consumer;

import java.io.InputStream;
import java.util.Date;

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
  public static final String DEFAULT_CONSUMER_CLASS_NAME = 
      "com.inmobi.messaging.consumer.databus.DatabusConsumer";
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
    return create(loadConfigFromClassPath());
  }

  /**
   * Creates concrete class extending {@link AbstractMessageConsumer} given by
   * name {@value #CONSUMER_CLASS_NAME_KEY}, by loading the properties from
   * configuration file named {@value #MESSAGE_CLIENT_CONF_FILE} from classpath.
   * 
   * Initializes the consumer class with passed configuration, topicName with
   * the value of {@value #TOPIC_NAME_KEY}, consumerName with the value of 
   * {@value #CONSUMER_NAME_KEY} and the startTime.
   *
   * @param startTime The startTime from which messages should be read
   * 
   * @return {@link MessageConsumer} concrete object
   */
  public static MessageConsumer create(Date startTime) {
    return create(loadConfigFromClassPath(), startTime);
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
    return create(ClientConfig.load(confFile));
  }

  /**
   * Creates concrete class extending {@link AbstractMessageConsumer} given by
   * name {@value #CONSUMER_CLASS_NAME_KEY}, by loading the passed
   * configuration file.
   * 
   * Initializes the consumer class with passed configuration, topicName with
   * the value of {@value #TOPIC_NAME_KEY}, consumerName with the value of 
   * {@value #CONSUMER_NAME_KEY} and the startTime.
   *
   * @param confFile The file name
   *  
   * @return {@link MessageConsumer} concrete object
   */
  public static MessageConsumer create(String confFile, Date startTime) {
    return create(ClientConfig.load(confFile), startTime);
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
    String consumerName = config.getString(CONSUMER_CLASS_NAME_KEY,
        DEFAULT_CONSUMER_CLASS_NAME);
    return create(config, consumerName);
  }
  
  /**
   * Creates concrete class extending {@link AbstractMessageConsumer} given by
   * name {@value #CONSUMER_CLASS_NAME_KEY}, using the passed configuration
   * object.
   * 
   * Initializes the consumer class with passed configuration, topicName with
   * the value of {@value #TOPIC_NAME_KEY}, consumerName with the value of 
   * {@value #CONSUMER_NAME_KEY} and the startTime.
   *
   * @param config {@link ClientConfig} object
   * @param startTime The startTime from which messages should be read
   *
   * @return {@link MessageConsumer} concrete object
   */
   public static MessageConsumer create(ClientConfig config, Date startTime) {
     String consumerName = config.getString(CONSUMER_CLASS_NAME_KEY,
         DEFAULT_CONSUMER_CLASS_NAME);
     return create(config, consumerName, startTime);
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
    return create(config, consumerClassName, null);
  }

  /**
   * Creates concrete class extending {@link AbstractMessageConsumer} with
   * passed class name and using the passed configuration object.
   * 
   * Initializes the consumer class with passed configuration, topicName with
   * the value of {@value #TOPIC_NAME_KEY}, consumerName with the value of 
   * {@value #CONSUMER_NAME_KEY} and the startTime.
   *
   * @param config {@link ClientConfig} object
   * @param consumerClassName The class name of the consumer implementation
   * @param startTime The startTime from which messages should be read
   * 
   * @return {@link MessageConsumer} concrete object
   */
  public static MessageConsumer create(ClientConfig config,
                                       String consumerClassName, 
                                       Date startTime) {
    return create(config, consumerClassName, config.getString(TOPIC_NAME_KEY),
        config.getString(CONSUMER_NAME_KEY), startTime);
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
    return create(config, consumerClassName, topicName, consumerName, null);
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
   * @param consumerName The name of the consumer being consumed
   * @param startTime The starting time from which messages should be consumed.
   * 
   * @return {@link MessageConsumer} concrete object
   */
  public static MessageConsumer create(ClientConfig config,
                                       String consumerClassName,
                                       String topicName,
                                       String consumerName,
                                       Date startTime) {
    AbstractMessageConsumer consumer = createAbstractConsumer(consumerClassName);
    if (topicName == null) {
      throw new RuntimeException("Could not create consumer with null topic" +
        " name");
    }
    if (consumerName == null) {
      throw new RuntimeException("Could not create consumer with null consumer"
        + " name");
    }
    config.set(CONSUMER_CLASS_NAME_KEY, consumerClassName);
    config.set(TOPIC_NAME_KEY, topicName);
    config.set(CONSUMER_NAME_KEY, consumerName);
    consumer.init(topicName, consumerName, startTime, config);
    return consumer;
  }

  private static ClientConfig loadConfigFromClassPath() {
    InputStream in = ClientConfig.class.getClassLoader().getResourceAsStream(
        MESSAGE_CLIENT_CONF_FILE);
    if (in == null) {
      throw new RuntimeException("could not load conf file "
          + MESSAGE_CLIENT_CONF_FILE + " from classpath.");
    }
    return ClientConfig.load(in);
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
  

}
