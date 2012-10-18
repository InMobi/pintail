package com.inmobi.messaging.netty;

public interface ScribePublisherConfiguration {

  public static final String maxConnectionRetriesConfig =
      "scribe.maxconnectionretries";
  public static final int DEFAULT_MAX_CONNECTION_RETRIES = 3;

  public static final String hostNameConfig = "scribe.host";
  public static final String DEFAULT_HOST = "localhost";

  public static final String portConfig = "scribe.port";
  public static final int DEFAULT_PORT = 1111;

  public static final String backOffSecondsConfig = "scribe.backoffSeconds";
  public static final int DEFAULT_BACKOFF = 5;

  public static final String timeoutSecondsConfig = "scribe.timeoutSeconds";
  public static final int DEFAULT_TIMEOUT = 5;

  public static final String drainOnCloseConfig = "scribe.drainOnClose";
  public static final boolean DEFAULT_DRAINONCLOSE = true;

  public static final String resendAckLostConfig = "scribe.resend.ackLost";
  public static final boolean DEFAULT_RESEND_ACKLOST = true;

  public static final String scribeMessageQueueSize = "scribe.message.queuesize";
  public static final int DEFAULT_MSG_QUEUE_SIZE = 10000;

  public static final String scribeMessageAckQueueSize = 
      "scribe.message.ack.queuesize";
  public static final int DEFAULT_MSG_ACK_QUEUE_SIZE = 10000;

}
