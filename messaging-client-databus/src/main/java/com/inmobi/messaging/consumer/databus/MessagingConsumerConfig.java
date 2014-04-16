package com.inmobi.messaging.consumer.databus;

import org.apache.hadoop.mapred.TextInputFormat;

import com.inmobi.messaging.checkpoint.FSCheckpointProvider;


public interface MessagingConsumerConfig {

  public static final String queueSizeConfig = "messaging.consumer.buffer.size";
  public static final int DEFAULT_QUEUE_SIZE = 5000;

  public static final String chkProviderConfig =
      "messaging.consumer.chkpoint.provider.classname";
  public static final String DEFAULT_CHK_PROVIDER = FSCheckpointProvider.class
      .getName();

  public static final String checkpointDirConfig =
      "messaging.consumer.checkpoint.dir";
  public static final String DEFAULT_CHECKPOINT_DIR = ".";

  public static final String consumerPrincipal =
      "messaging.consumer.principal.name";
  public static final String consumerKeytab =
      "messaging.consumer.keytab.path";

  public static final String waitTimeForFileCreateConfig =
      "messaging.consumer.waittime.forfilecreate";
  public static final long DEFAULT_WAIT_TIME_FOR_FILE_CREATE = 1000; //1 second

  public static final String inputFormatClassNameConfig =
      "messaging.consumer.inputformat.classname";
  public static final String DEFAULT_INPUT_FORMAT_CLASSNAME =
      TextInputFormat.class.getCanonicalName();

  @Deprecated
  public static final String retentionConfig =
      "messaging.consumer.topic.retention.inhours";

  public static final String relativeStartTimeConfig =
      "messaging.consumer.relative.starttime.inminutes";

  public static final String stopDateConfig =
      "messaging.consumer.absolute.stoptime";

  public static final String startOfStreamConfig =
      "messaging.consumer.startofstream";

  public static final boolean DEFAULT_START_OF_STREAM = false;

  public static final String hadoopConfigFileKey =
      "messaging.consumer.hadoop.conf";

  /**
   * The consumer id is used in case of groups. The number associated with
   * consumer in the group, for eg. 2/5
   */
  public static final String consumerIdInGroupConfig =
      "messaging.consumer.group.membership";
  public static final String DEFAULT_CONSUMER_ID = "1/1";

  public static final String readFromLocalStreamConfig =
      "messaging.consumer.read.localstream";
  public static final boolean DEFAULT_READ_LOCAL_STREAM = true;

  public static final String clustersNameConfig =
      "messaging.consumer.clusternames";
}
