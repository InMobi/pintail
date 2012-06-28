package com.inmobi.messaging.consumer.databus;

import com.inmobi.databus.FSCheckpointProvider;

/**
 * Configuration properties and their default values for {@link DatabusConsumer}
 */
public interface DatabusConsumerConfig {

  public static final String queueSizeConfig = "databus.consumer.buffer.size";
  public static final int DEFAULT_QUEUE_SIZE = 5000;

  public static final String databusConfigFileKey = "databus.conf";
  public static final String DEFAULT_DATABUS_CONFIG_FILE = "databus.xml";

  public static final String databusClustersConfig = "databus.consumer.clusters";

  public static final String databusChkProviderConfig = 
      "databus.consumer.chkpoint.provider.classname";
  public static final String DEFAULT_CHK_PROVIDER = FSCheckpointProvider.class
  .getName();

  public static final String checkpointDirConfig = 
  "databus.consumer.checkpoint.dir";
  public static final String DEFAULT_CHECKPOINT_DIR = ".";

  public static final String databusConsumerPrincipal = 
      "databus.consumer.principal.name";
  public static final String databusConsumerKeytab = 
      "databus.consumer.keytab.path";

  public static final String databusStreamType = "databus.consumer.stream.type";
  public static final String DEFAULT_STREAM_TYPE = StreamType.COLLECTOR.name();

  public static final String waitTimeForFlushConfig = 
  "databus.consumer.waittime.forcollectorflush";
  public static final long DEFAULT_WAIT_TIME_FOR_FLUSH = 5000; // 5 second

  public static final String waitTimeForFileCreateConfig = 
  "databus.consumer.waittime.forfilecreate";
  public static final long DEFAULT_WAIT_TIME_FOR_FILE_CREATE = 1000; //1 second
}
