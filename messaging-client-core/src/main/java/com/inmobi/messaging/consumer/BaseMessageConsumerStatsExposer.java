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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base metrics class for MessageConsumer.
 */
public class BaseMessageConsumerStatsExposer extends
    AbstractMessageConsumerStatsExposer {
  private final AtomicLong numMessagesConsumed = new AtomicLong(0);
  private final AtomicLong numMarkCalls = new AtomicLong(0);
  private final AtomicLong numResetCalls = new AtomicLong(0);
  private final AtomicLong numOfTimeoutsOnNextCall = new AtomicLong(0);

  public BaseMessageConsumerStatsExposer(String topicName, String consumerName)
  {
    super(topicName, consumerName);
  }

  public void incrementMessagesConsumed() {
    numMessagesConsumed.incrementAndGet();
  }

  public void incrementMarkCalls() {
    numMarkCalls.incrementAndGet();
  }

  public void incrementResetCalls() {
    numResetCalls.incrementAndGet();
  }

  public void incrementTimeOutsOnNext() {
    numOfTimeoutsOnNextCall.incrementAndGet();
  }

  public long getNumMessagesConsumed() {
    return numMessagesConsumed.get();
  }

  public long getNumMarkCalls() {
    return numMarkCalls.get();
  }

  public long getNumResetCalls() {
    return numResetCalls.get();
  }

  public long getNumOfTiemOutsOnNext() {
    return numOfTimeoutsOnNextCall.get();
  }

  @Override
  protected void addToStatsMap(Map<String, Number> statsMap) {
    statsMap.put(MESSAGES_CONSUMED, getNumMessagesConsumed());
    statsMap.put(MARK_CALLS, getNumMarkCalls());
    statsMap.put(RESET_CALLS, getNumResetCalls());
    statsMap.put(TIMEOUTS_NEXT_CALL, getNumOfTiemOutsOnNext());
  }
}
