package com.inmobi.messaging.consumer;

/*
 * #%L
 * messaging-client-core
 * %%
 * Copyright (C) 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
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
  public static final String EMITTER_CONF_FILE_KEY =
      "consumer.statemitter.filename";
  public static final String ABSOLUTE_START_TIME =
      "messaging.consumer.absolute.starttime";

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
   * @throws IOException
   */
  public static MessageConsumer create() throws IOException {
    return create(ClientConfig.loadFromClasspath(MESSAGE_CLIENT_CONF_FILE));
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
   * @throws IOException
   */
  public static MessageConsumer create(Date startTime) throws IOException {
    return create(ClientConfig.loadFromClasspath(MESSAGE_CLIENT_CONF_FILE),
        startTime);
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
   * @throws IOException
   */
  public static MessageConsumer create(String confFile) throws IOException {
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
   * @throws IOException
   */
  public static MessageConsumer create(String confFile, Date startTime)
      throws IOException {
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
   * @throws IOException
   */
  public static MessageConsumer create(ClientConfig config) throws IOException {
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
   * @throws IOException
   */
  public static MessageConsumer create(ClientConfig config, Date startTime)
      throws IOException {
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
   * @throws IOException
   */
  public static MessageConsumer create(ClientConfig config,
      String consumerClassName) throws IOException {
    String absoluteStartTimeStr = config.getString(ABSOLUTE_START_TIME);
    Date absoluteStartTime = AbstractMessageConsumer.getDateFromString(
        absoluteStartTimeStr);
    return create(config, consumerClassName, absoluteStartTime);
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
   * @throws IOException
   */
  public static MessageConsumer create(ClientConfig config,
      String consumerClassName,
      Date startTime) throws IOException {
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
   * @throws IOException
   */
  public static MessageConsumer create(ClientConfig config,
      String consumerClassName,
      String topicName,
      String consumerName) throws IOException {
    String absoluteStartTimeStr = config.getString(ABSOLUTE_START_TIME);
    Date absoluteStartTime = AbstractMessageConsumer.getDateFromString(
        absoluteStartTimeStr);
    return create(config, consumerClassName, topicName, consumerName,
        absoluteStartTime);
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
   * @throws IOException
   */
  public static MessageConsumer create(ClientConfig config,
      String consumerClassName,
      String topicName,
      String consumerName,
      Date startTime) throws IOException {
    AbstractMessageConsumer consumer = createAbstractConsumer(consumerClassName);
    if (topicName == null) {
      throw new RuntimeException("Could not create consumer with null topic"
          + " name");
    }
    if (consumerName == null) {
      throw new RuntimeException("Could not create consumer with null consumer"
          + " name");
    }
    if (startTime != null) {
      config.set(ABSOLUTE_START_TIME,
          AbstractMessageConsumer.minDirFormat.get().format(startTime));
    }
    config.set(CONSUMER_CLASS_NAME_KEY, consumerClassName);
    config.set(TOPIC_NAME_KEY, topicName);
    config.set(CONSUMER_NAME_KEY, consumerName);
    consumer.init(topicName, consumerName, startTime, config);
    return consumer;
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
