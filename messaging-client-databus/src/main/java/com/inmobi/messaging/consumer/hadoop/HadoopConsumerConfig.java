package com.inmobi.messaging.consumer.hadoop;

import com.inmobi.messaging.consumer.databus.MessagingConsumerConfig;

public interface HadoopConsumerConfig extends MessagingConsumerConfig {

  public static final String rootDirsConfig =
      "hadoop.consumer.root.dirs";
  
}
