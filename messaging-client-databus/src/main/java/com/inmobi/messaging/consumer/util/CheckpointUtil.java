package com.inmobi.messaging.consumer.util;

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

import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;

public class CheckpointUtil {

  public static void main(String [] args) throws Exception {
    String confFile = null;
    if (args.length > 0) {
      confFile = args[0];
    }
    System.setProperty("consumer.checkpoint.migrate", "true");
    MessageConsumer consumer;
    if (confFile != null) {
      consumer = MessageConsumerFactory.create(confFile);
    } else {
      consumer = MessageConsumerFactory.create();
    }
    consumer.mark();
    System.setProperty("consumer.checkpoint.migrate", "false");
    consumer.close();
  }
}


