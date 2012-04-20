package com.inmobi.messaging;

/*
 * Factory to create concrete MessagePublisher instance.
 */
public class MessagePublisherFactory {

  /*
   * Creates MessagePublisher by loading the messaging-client-conf.properties 
   * config file from classpath.
   */
  public static MessagePublisher create() {
    ClientConfig config = ClientConfig.load();
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
        .getString(ClientConfig.PUBLISHER_CLASS_NAME_KEY);
    AbstractMessagePublisher publisher = null;
    try {
      clazz = Class.forName(publisherName);
      publisher = (AbstractMessagePublisher) clazz.newInstance();

    } catch (Exception e) {
      throw new RuntimeException("Could not create message publisher "
          + config.getString(ClientConfig.PUBLISHER_CLASS_NAME_KEY), e);
    }
    publisher.init(config);
    return publisher;
  }
}
