package com.inmobi.messaging.util;

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
import java.util.Calendar;

import org.testng.annotations.Test;

public class AuditUtilTest {

  @Test
  public void testRemoveHeadersValid() {
    String data = "test data";
    ByteBuffer buffer = ByteBuffer.allocate(data.getBytes().length
        + AuditUtil.HEADER_LENGTH);
    buffer.put((byte) 1);
    buffer.put(AuditUtil.magicBytes);
    buffer.putLong(System.currentTimeMillis());
    buffer.putInt(data.length());
    buffer.put(data.getBytes());
    ByteBuffer returned = AuditUtil.removeHeader(buffer.array());
    assert (returned.capacity() + AuditUtil.HEADER_LENGTH == buffer.capacity());
  }

  @Test
  public void testRemoveHeadersInvalidVersion() {
    String data = "test data";
    ByteBuffer buffer = ByteBuffer.allocate(data.getBytes().length
        + AuditUtil.HEADER_LENGTH);
    buffer.put((byte) 10);
    buffer.put(AuditUtil.magicBytes);
    buffer.putLong(System.currentTimeMillis());
    buffer.putInt(data.length());
    buffer.put(data.getBytes());
    ByteBuffer returned = AuditUtil.removeHeader(buffer.array());
    assert (returned.capacity() == buffer.capacity());
    assert (returned.array().equals(buffer.array()));
  }

  @Test
  public void testRemoveHeaderInvalidMagicBytes() {
    String data = "test data";
    ByteBuffer buffer = ByteBuffer.allocate(data.getBytes().length
        + AuditUtil.HEADER_LENGTH);
    buffer.put((byte) 1);
    byte[] invalidMagic = {(byte) 1, (byte) 2, (byte) 3 };
    buffer.put(invalidMagic);
    buffer.putLong(System.currentTimeMillis());
    buffer.putInt(data.length());
    buffer.put(data.getBytes());
    ByteBuffer returned = AuditUtil.removeHeader(buffer.array());
    assert (returned.capacity() == buffer.capacity());
    assert (returned.array().equals(buffer.array()));
  }

  @Test
  public void testRemoveHeadersInvalidTimeStamp() {
    String data = "test data";
    ByteBuffer buffer = ByteBuffer.allocate(data.getBytes().length
        + AuditUtil.HEADER_LENGTH);
    buffer.put((byte) 1);
    buffer.put(AuditUtil.magicBytes);
    Calendar calendar = Calendar.getInstance();
    calendar.set(2010, 05, 27);
    buffer.putLong(calendar.getTimeInMillis());
    buffer.putInt(data.length());
    buffer.put(data.getBytes());
    ByteBuffer returned = AuditUtil.removeHeader(buffer.array());
    assert (returned.capacity() == buffer.capacity());
    assert (returned.array().equals(buffer.array()));
  }

  @Test
  public void testRemoveHeadersInvalidSize() {
    String data = "test data";
    ByteBuffer buffer = ByteBuffer.allocate(data.getBytes().length
        + AuditUtil.HEADER_LENGTH);
    buffer.put((byte) 1);
    buffer.put(AuditUtil.magicBytes);
    buffer.putLong(System.currentTimeMillis());
    buffer.putInt(data.length() - 2);
    buffer.put(data.getBytes());
    ByteBuffer returned = AuditUtil.removeHeader(buffer.array());
    assert (returned.capacity() == buffer.capacity());
    assert (returned.array().equals(buffer.array()));
  }

  @Test
  public void testDataLengthLessThanHeaders() {
    String data = "test data";
    ByteBuffer buffer = ByteBuffer.allocate(data.getBytes().length);
    buffer.put(data.getBytes());
    ByteBuffer returned = AuditUtil.removeHeader(buffer.array());
    assert (returned.capacity() == buffer.capacity());
    assert (returned.array().equals(buffer.array()));
  }

}
