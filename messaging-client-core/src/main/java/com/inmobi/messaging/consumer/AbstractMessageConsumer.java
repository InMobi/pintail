package com.inmobi.messaging.consumer;

/*
 * #%L
 * messaging-client-core
 * %%
 * Copyright (C) 2012 - 2014 InMobi
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

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.instrumentation.AbstractMessagingClientStatsExposer;
import com.inmobi.messaging.instrumentation.MessagingClientStatBuilder;

/**
 * Abstract class implementing {@link MessageConsumer} interface.
 *
 * It provides the access to configuration parameters({@link ClientConfig}) for
 * consumer interface
 *
 * It initializes topic name, consumer name and startTime. startTime is the time
 * from which messages should be consumed.
 * <ul>
 * <li>if no startTime is passed, messages will be consumed from last marked
 * position.
 * <li>
 * If there is no last marked position, messages will be consumed from the
 * starting of the available stream i.e. all messages that are not purged.
 * <li>If startTime or last marked position is beyond the retention period of
 * the stream, messages will be consumed from starting of the available stream.
 * </ul>
 */
public abstract class AbstractMessageConsumer implements MessageConsumer {

  private ClientConfig config;
  protected String topicName;
  protected String consumerName;
  protected Date startTime;
  private BaseMessageConsumerStatsExposer metrics;
  private MessagingClientStatBuilder statsEmitter = new MessagingClientStatBuilder();

  public static String minDirFormatStr = "yyyy" + File.separator + "MM"
      + File.separator + "dd" + File.separator + "HH" + File.separator + "mm";

  public static final ThreadLocal<DateFormat> minDirFormat =
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat(minDirFormatStr);
    }
  };

  /**
   * Initialize the consumer with passed configuration object
   *
   * @param config
   *          {@link ClientConfig} for the consumer
   * @throws IOException
   */
  protected void init(ClientConfig config) throws IOException {
    this.config = config;
  }

  protected abstract AbstractMessagingClientStatsExposer getMetricsImpl();

  protected abstract void doMark() throws IOException;

  protected abstract void doReset() throws IOException;

  protected abstract Message getNext()
      throws InterruptedException, EndOfStreamException;

  protected abstract Message getNext(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException;

  public synchronized Message next()
      throws InterruptedException, EndOfStreamException {
    Message msg = getNext();
    metrics.incrementMessagesConsumed();
    return msg;
  }

  public synchronized Message next(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException {
    Message msg = getNext(timeout, timeunit);
    if (msg != null) {
      metrics.incrementMessagesConsumed();
    } else {
      metrics.incrementTimeOutsOnNext();
    }
    return msg;
  }

  public synchronized void mark() throws IOException {
    if (isMarkSupported()) {
      doMark();
      metrics.incrementMarkCalls();
    }
  }

  public synchronized void reset() throws IOException {
    if (isMarkSupported()) {
      doReset();
      metrics.incrementResetCalls();
    }
  }

  /**
   * Initialize the consumer with passed configuration object, streamName and
   * consumerName and startTime.
   *
   * @param topicName
   *          Name of the topic being consumed
   * @param consumerName
   *          Name of the consumer
   * @param startTime
   *          Starting time from which messages should be consumed
   * @param config
   *          {@link ClientConfig} for the consumer
   * @throws IOException
   */
  public void init(String topicName, String consumerName, Date startTimestamp,
      ClientConfig config) throws IOException {
    this.topicName = topicName;
    this.consumerName = consumerName;
    this.startTime = startTimestamp;
    // do not accept start time in future
    if (startTime != null
        && startTime.after(new Date(System.currentTimeMillis()))) {
      throw new IllegalArgumentException("Future start time is not accepted");
    }
    init(config);
    metrics = (BaseMessageConsumerStatsExposer) getMetricsImpl();
    String emitterConfig = config
        .getString(MessageConsumerFactory.EMITTER_CONF_FILE_KEY);
    if (emitterConfig != null) {
      statsEmitter.init(emitterConfig);
      statsEmitter.add(metrics);
    }
  }

  /**
   * Get the configuration of the consumer.
   *
   * @return {@link ClientConfig} object
   */
  public ClientConfig getConfig() {
    return this.config;
  }

  /**
   * Get the topic name being consumed.
   *
   * @return String topicName
   */
  public String getTopicName() {
    return topicName;
  }

  /**
   * Get the consumer name
   *
   * @return String consumerName
   */
  public String getConsumerName() {
    return consumerName;
  }

  /**
   * Get the starting time of the consumption.
   *
   * @return Date object
   */
  public Date getStartTime() {
    return startTime;
  }

  /**
   * Get the consumer metrics object
   *
   * @return MessageConsumerMetrics object
   */
  public AbstractMessagingClientStatsExposer getMetrics() {
    return metrics;
  }

  public static Date getDateFromString(String timeStamp) {
    if (timeStamp != null) {
      String dateString = timeStamp.substring(0,
          minDirFormatStr.length());
      try {
        return minDirFormat.get().parse(dateString);
      } catch (java.text.ParseException e) {
        throw new IllegalArgumentException("Incorrect format of "
            + "startTime/stopTime passed " +  " Absolute startTime/stopTime "
            + "should be in this format: " +  minDirFormatStr);
      }
    }
    return null;
  }

  public static String getStringFromDate(Date date) {
    return minDirFormat.get().format(date);
  }

  /**
   * Add statsExposer to the emitter.
   *
   * @param statsExposer
   */
  protected void addStatsExposer(
      AbstractMessageConsumerStatsExposer statsExposer) {
    statsEmitter.add(statsExposer);
  }

  /**
   * Remove statsExposer from the emitter.
   *
   * @param statsExposer
   */
  protected void removeStatsExposer(
      AbstractMessageConsumerStatsExposer statsExposer) {
    statsEmitter.remove(statsExposer);
  }

  /**
   * Get the client stats object
   *
   * @return MessagingClientStats object
   */
  MessagingClientStatBuilder getStatsBuilder() {
    return statsEmitter;
  }

  @Override
  public void close() {
    statsEmitter.remove(metrics);
  }
}
