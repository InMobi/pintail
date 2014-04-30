package com.inmobi.messaging.thrift;

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

import java.io.UnsupportedEncodingException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

class TStringByteProtocol extends TBinaryProtocol {

  public TStringByteProtocol(TTransport trans) {
    super(trans);
  }

  @Override
  public void writeString(String str) throws TException {
    try {
      byte[] dat = str.getBytes("ISO-8859-1");
      writeI32(dat.length);
      trans_.write(dat, 0, dat.length);
    } catch (UnsupportedEncodingException uex) {
      throw new TException("JVM DOES NOT SUPPORT ISO-8859-1");
    }
  }

  @Override
  public String readString() throws TException {
    throw new TException("Unimplemented");
  }
}
