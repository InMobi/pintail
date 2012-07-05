package com.inmobi.messaging.consumer.hadoop;

import org.apache.hadoop.mapred.TextInputFormat;

import com.inmobi.messaging.consumer.databus.MessagingConsumerConfig;

public interface HadoopConsumerConfig extends MessagingConsumerConfig {

  public static final String retentionConfig =
      "messaging.consumer.retention.inhours";
  public static final int DEFAULT_RETENTION_HOURS = 24;

  public static final String rootDirsConfig =
      "messaging.consumer.root.dirs";
  
  public static final String hadoopConfigFileKey =
      "messaging.consumer.hadoop.conf";
  
  public static final String inputFormatClassNameConfig =
      "messaging.consumer.input.format.class.name";
  public static final String DEFAULT_INPUT_FORMAT_CLASSNAME =
      TextInputFormat.class.getCanonicalName();
}
