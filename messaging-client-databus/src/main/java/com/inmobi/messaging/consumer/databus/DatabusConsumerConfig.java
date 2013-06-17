package com.inmobi.messaging.consumer.databus;

/**
 * Configuration properties and their default values for {@link DatabusConsumer}
 */
public interface DatabusConsumerConfig extends MessagingConsumerConfig {

  public static final String databusRootDirsConfig = "databus.consumer.rootdirs";

  public static final String databusStreamType = "databus.consumer.stream.type";
  public static final String DEFAULT_STREAM_TYPE = StreamType.COLLECTOR.name();

  public static final String waitTimeForFlushConfig =
  "databus.consumer.waittime.forcollectorflush";
  public static final long DEFAULT_WAIT_TIME_FOR_FLUSH = 5000; // 5 second
}
