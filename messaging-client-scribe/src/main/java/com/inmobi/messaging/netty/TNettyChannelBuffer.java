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

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Thrift transport based on JBoss Netty's ChannelBuffers
 *
 * @author <a href="http://www.pedantique.org/">Carl Bystr&ouml;m</a>
 *         https://github.com/cgbystrom/netty-tools
 *
 *         Licensed under the MIT license
 */
public class TNettyChannelBuffer extends TTransport {
  private ChannelBuffer inputBuffer;
  private ChannelBuffer outputBuffer;

  public TNettyChannelBuffer(ChannelBuffer input, ChannelBuffer output) {
    this.inputBuffer = input;
    this.outputBuffer = output;
  }

  @Override
  public boolean isOpen() {
    // Buffer is always open
    return true;
  }

  @Override
  public void open() throws TTransportException {
    // Buffer is always open
  }

  @Override
  public void close() {
    // Buffer is always open
  }

  @Override
  public int read(byte[] buffer, int offset, int length)
      throws TTransportException {
    int readableBytes = inputBuffer.readableBytes();
    int bytesToRead = length > readableBytes ? readableBytes : length;

    inputBuffer.readBytes(buffer, offset, bytesToRead);
    return bytesToRead;
  }

  @Override
  public void write(byte[] buffer, int offset, int length)
      throws TTransportException {
    outputBuffer.writeBytes(buffer, offset, length);
  }

  public ChannelBuffer getInputBuffer() {
    return inputBuffer;
  }

  public ChannelBuffer getOutputBuffer() {
    return outputBuffer;
  }
}
