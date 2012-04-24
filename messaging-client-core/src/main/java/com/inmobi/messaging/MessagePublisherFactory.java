package com.inmobi.messaging;

import java.io.InputStream;

/*
 * Factory to create concrete MessagePublisher instance.
 */
public class MessagePublisherFactory {

  public static final String MESSAGE_CLIENT_CONF_FILE = "messaging-publisher-conf.properties";
  public static final String PUBLISHER_CLASS_NAME_KEY = "publisher.classname";
  public static final String EMITTER_CONF_FILE_KEY = "statemitter.filename";

  /*
   * Creates MessagePublisher by loading the config file from classpath.
   */
  public static MessagePublisher create() {
    InputStream in = ClientConfig.class
        .getClassLoader().getResourceAsStream(MESSAGE_CLIENT_CONF_FILE);
    if (in == null) {
      throw new RuntimeException("could not load conf file " + 
          MESSAGE_CLIENT_CONF_FILE + " from classpath.");
    }
    ClientConfig config = ClientConfig.load(in);
    return create(config);
  }

  /*
   * Creates MessagePublisher by loading the passed config file.
   */
  public static MessagePublisher create(String confFile) {
    ClientConfig config = ClientConfig.load(confFile);
    return create(config);
  }

  /*
   * Creates MessagePublisher using the passed config file.
   */
  public static MessagePublisher create(ClientConfig config) {
    Class<?> clazz;
    String publisherName = config
        .getString(PUBLISHER_CLASS_NAME_KEY);
    AbstractMessagePublisher publisher = null;
    try {
      clazz = Class.forName(publisherName);
      publisher = (AbstractMessagePublisher) clazz.newInstance();

    } catch (Exception e) {
      throw new RuntimeException("Could not create message publisher "
          + config.getString(PUBLISHER_CLASS_NAME_KEY), e);
    }
    publisher.init(config);
    return publisher;
  }
}
