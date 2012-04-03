package com.inmobi.messaging;

public class MessagePublisherFactory {

  public static MessagePublisher create() {
    ClientConfig config = ClientConfig.load();
    return create(config);
  }

  public static MessagePublisher create(String confFile) {
    ClientConfig config = ClientConfig.load(confFile);
    return create(config);
  }

  public static MessagePublisher create(ClientConfig config) {
    Class<?> clazz;
    String publisherName = config
        .getString(ClientConfig.PUBLISHER_CLASS_NAME_KEY);
    MessagePublisher publisher = null;
    try {
      clazz = Class.forName(publisherName);
      publisher = (MessagePublisher) clazz.newInstance();

    } catch (Exception e) {
      throw new RuntimeException("Could not create message publisher "
          + config.getString(ClientConfig.PUBLISHER_CLASS_NAME_KEY), e);
    }
    publisher.init(config);
    return publisher;
  }
}
