package com.inmobi.messaging.netty;

/*
 * #%L
 * messaging-client-scribe
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

public interface ScribePublisherConfiguration {

  public static final String hostNameConfig = "scribe.host";
  public static final String DEFAULT_HOST = "localhost";

  public static final String portConfig = "scribe.port";
  public static final int DEFAULT_PORT = 1111;

  public static final String backOffSecondsConfig = "scribe.backoffSeconds";
  public static final int DEFAULT_BACKOFF = 5;

  public static final String timeoutSecondsConfig = "scribe.timeoutSeconds";
  public static final int DEFAULT_TIMEOUT = 5;

  public static final String resendAckLostConfig = "scribe.resend.ackLost";
  public static final boolean DEFAULT_RESEND_ACKLOST = true;

  public static final String messageQueueSizeConfig = "scribe.message.queuesize";
  public static final int DEFAULT_MSG_QUEUE_SIZE = 50000;

  public static final String ackQueueSizeConfig = "scribe.ack.queuesize";
  public static final int DEFAULT_ACK_QUEUE_SIZE = 1000;

  public static final String retryConfig = "scribe.enable.retries";
  public static final boolean DEFAULT_ENABLE_RETRIES = true;

  public static final String asyncSenderSleepMillis =
      "scribe.async.sender.sleep.millis";
  public static final long DEFAULT_ASYNC_SENDER_SLEEP = 10;

  public static final String drainRetriesOnCloseConfig =
      "scribe.numdrains.onclose";
  public static final int DEFAULT_NUM_DRAINS_ONCLOSE = -1;
}
