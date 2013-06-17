package com.inmobi.messaging.publisher;

import java.io.IOException;

import com.inmobi.messaging.ClientConfig;

/**
 * Factory to create concrete MessagePublisher instance.
 */
public class MessagePublisherFactory {

  public static final String MESSAGE_CLIENT_CONF_FILE =
      "messaging-publisher-conf.properties";
  public static final String PUBLISHER_CLASS_NAME_KEY = "publisher.classname";
  public static final String EMITTER_CONF_FILE_KEY = "statemitter.filename";
  public static final String DEFAULT_PUBLISHER_CLASS_NAME =
      "com.inmobi.messaging.netty.ScribeMessagePublisher";

  /**
   * Creates concrete class extending {@link AbstractMessagePublisher} given by
   * name {@value #PUBLISHER_CLASS_NAME_KEY}, by loading the configuration file
   * named {@value #MESSAGE_CLIENT_CONF_FILE} from classpath.
   * Also initializes the publisher class with passed configuration.
   *
   * @return {@link MessagePublisher} concrete object
   * @throws IOException
   */
  public static MessagePublisher create() throws IOException {
    return create(ClientConfig.loadFromClasspath(MESSAGE_CLIENT_CONF_FILE));
  }

  /**
   * Creates concrete class extending {@link AbstractMessagePublisher} given by
   * name {@value #PUBLISHER_CLASS_NAME_KEY}, by loading the passed config file.
   * Also initializes the publisher class with passed configuration.
   *
   * @param confFile The configuration File name.
   *
   * @return {@link MessagePublisher} concrete object
   * @throws IOException
   */
  public static MessagePublisher create(String confFile) throws IOException {
    ClientConfig config = ClientConfig.load(confFile);
    return create(config);
  }

  /**
   * Creates concrete class extending {@link AbstractMessagePublisher} given by
   * name {@value #PUBLISHER_CLASS_NAME_KEY}, using the passed configuration
   * object.
   *  Also initializes the publisher class with passed configuration object.
   *
   * @param config The {@link ClientConfig}
   *
   * @return {@link MessagePublisher} concrete object
   * @throws IOException 
   */
  public static MessagePublisher create(ClientConfig config)
      throws IOException {
    String publisherName = config
        .getString(PUBLISHER_CLASS_NAME_KEY, DEFAULT_PUBLISHER_CLASS_NAME);
    return create(config, publisherName);
  }

  /**
   * Creates concrete class extending {@link AbstractMessagePublisher} with
   * passed name and using the passed configuration object.
   * Also initializes the publisher class with passed configuration object.
   *
   * @param config The {@link ClientConfig}
   *
   * @return {@link MessagePublisher} concrete object
   * @throws IOException
   */
  public static MessagePublisher create(ClientConfig config,
       String publisherClassName) throws IOException {
    Class<?> clazz;
    AbstractMessagePublisher publisher = null;
    try {
      clazz = Class.forName(publisherClassName);
      publisher = (AbstractMessagePublisher) clazz.newInstance();

    } catch (Exception e) {
      throw new RuntimeException("Could not create message publisher "
          + config.getString(PUBLISHER_CLASS_NAME_KEY), e);
    }
    publisher.init(config);
    return publisher;
  }

}
