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

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.Timer;

public class ScribePipelineFactory implements ChannelPipelineFactory {
  private final ScribeHandler handler;
  private final int MAX_FRAME_SIZE = 512 * 1024;
  private final int timeout;
  private final Timer timer;

  public ScribePipelineFactory(ScribeHandler handler, int socketTimeout,
      Timer timer) {
    this.handler = handler;
    this.timeout = socketTimeout;
    this.timer = timer;
  }

  public ChannelPipeline getPipeline() throws Exception {
    ChannelPipeline pipeline = pipeline();
    pipeline.addLast("timeout", new ReadTimeoutHandler(timer, timeout));
    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
        MAX_FRAME_SIZE, 0, 4, 0, 4));
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
    pipeline.addLast("thriftHandler", handler);
    return pipeline;
  }

}
