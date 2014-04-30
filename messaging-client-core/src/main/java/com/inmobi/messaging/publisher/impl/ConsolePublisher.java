package com.inmobi.messaging.publisher.impl;

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

import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;

public class ConsolePublisher extends AbstractMessagePublisher {

  @Override
  protected void publish(Map<String, String> headers, Message m) {
    System.out.println(m.getData().asCharBuffer().toString());
  }

}
