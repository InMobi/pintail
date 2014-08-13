package com.inmobi.messaging.netty;

/*
 * #%L
 * messaging-client-scribe
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.Message;

/**
 */
public class ScribeBlockingTopicPublisher extends ScribeTopicPublisher {
  private static final Log LOG = LogFactory.getLog(ScribeTopicPublisher.class);

  @Override
  protected boolean addToSend(final Message m) {
    try {
      toBeSent.put(m);
      return true;
    } catch (InterruptedException e) {
      LOG.error("Error while waiting for free space in queue. Message dropped :( ");
      stats.accumulateOutcomeWithDelta(Outcome.LOST, 0);
      return false;
    }
  }
}
