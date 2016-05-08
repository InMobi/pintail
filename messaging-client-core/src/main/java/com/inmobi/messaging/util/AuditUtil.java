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
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.messaging.Message;

public class AuditUtil {
  static final byte[] magicBytes = {(byte) 0xAB, (byte) 0xCD, (byte) 0xEF };
  public static final String AUDIT_STREAM_TOPIC_NAME = "_audit";
  private static final byte[] versions = {1, 2};
  public static final int currentVersion = 2;
  private static final Log LOG = LogFactory.getLog(AuditUtil.class);
  public static final int HEADER_LENGTH = 16;
  private static final long BASE_TIME = 1356998400000L;
  public static final String DATE_FORMAT = "dd-MM-yyyy-HH:mm";
  private static final int POSITION_OF_TIMESTAMP = 4;

  public static void attachHeaders(Message m, Long timestamp) {
    byte[] b = m.getData().array();
    int messageSize = b.length;
    int totalSize = messageSize + HEADER_LENGTH;
    ByteBuffer buffer = ByteBuffer.allocate(totalSize);

    // writing version
    buffer.put((byte) currentVersion);
    // writing magic bytes
    buffer.put(magicBytes);
    // writing timestamp
    long time = timestamp;
    buffer.putLong(time);

    // writing message size
    buffer.putInt(messageSize);
    // writing message
    buffer.put(b);
    buffer.rewind();
    m.set(buffer);
    // return new Message(buffer);

  }

  public static ByteBuffer removeHeader(byte[] data) {
    if (isValidHeaders(data)) {
      return ByteBuffer.wrap(Arrays.copyOfRange(data, HEADER_LENGTH,
          data.length));
    } else {
      return ByteBuffer.wrap(data);
    }
  }

  private static boolean isValidHeaders(byte[] data) {
    if (data.length < HEADER_LENGTH) {
      LOG.debug("Total size of data in message is less than length of headers");
      return false;
    }
    ByteBuffer buffer = ByteBuffer.wrap(data);
    return isValidVersion(buffer) && isValidMagicBytes(buffer)
        && isValidTimestamp(buffer) && isValidSize(buffer);
  }

  private static boolean isValidVersion(ByteBuffer buffer) {
    byte versionFound = buffer.get();
    for (byte version : versions) {
      if (versionFound == version) {
        return true;
      }
    }
    LOG.debug("Invalid version in headers");
    return false;
  }

  private static boolean isValidMagicBytes(ByteBuffer buffer) {
    // compare all 3 magicBytes
    byte[] mBytesRead = new byte[3];
    buffer.get(mBytesRead);
    if (mBytesRead[0] != magicBytes[0] || mBytesRead[1] != magicBytes[1]
        || mBytesRead[2] != magicBytes[2]) {
      LOG.debug("Invalid magic bytes");
      return false;
    }
    return true;
  }

  private static boolean isValidTimestamp(ByteBuffer buffer) {
    long timestamp = buffer.getLong();
    if (timestamp < BASE_TIME) {
      LOG.debug("Invalid TimeStamp in headers [" + timestamp + "]");
      return false;
    }
    return true;
  }

  private static boolean isValidSize(ByteBuffer buffer) {
    int messageSize = buffer.getInt();
    if (buffer.limit() != messageSize + HEADER_LENGTH) {
      LOG.debug("Invalid size of message in headers;found ["
          + (HEADER_LENGTH + messageSize) + "] expected [" + buffer.limit()
          + "]");
      return false;
    }
    return true;
  }

  public static long getTimestamp(byte[] msg) {
    if (isValidHeaders(msg)) {
      ByteBuffer buffer = ByteBuffer.wrap(msg);
      return buffer.getLong(POSITION_OF_TIMESTAMP);
    } else
      return -1;
  }
}
