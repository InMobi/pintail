package com.inmobi.messaging.publisher;

/*
 * #%L
 * messaging-client-core
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

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.AuditUtil;

public class TestAuditService {
  private static final byte[] magicbytes = {(byte) 0xAB, (byte) 0xCD,
      (byte) 0xEF };

  @Test
  public void testAttachHeaders() {
    Message m = new Message("test data".getBytes());
    Long timestamp = System.currentTimeMillis();
    AuditUtil.attachHeaders(m, timestamp);
    ByteBuffer buffer = m.getData();
    buffer.rewind();
    int version = buffer.get();
    assert (version == 2);
    byte[] bytes = new byte[3];
    buffer.get(bytes);
    assert (bytes[0] == magicbytes[0]);
    assert (bytes[1] == magicbytes[1]);
    assert (bytes[2] == magicbytes[2]);
    assert (buffer.getLong() == timestamp);
    int msgSize = buffer.getInt();
    byte[] msg = new byte[msgSize];
    buffer.get(msg);
    assert (new String(msg).equals("test data"));
    buffer.rewind();
    byte[] withoutHeaders = AuditUtil.removeHeader(buffer.array()).array();
    assert (new String(withoutHeaders).equals("test data"));

  }

}
