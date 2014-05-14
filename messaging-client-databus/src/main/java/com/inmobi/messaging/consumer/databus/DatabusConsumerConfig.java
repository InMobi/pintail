package com.inmobi.messaging.consumer.databus;

/*
 * #%L
 * messaging-client-databus
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
