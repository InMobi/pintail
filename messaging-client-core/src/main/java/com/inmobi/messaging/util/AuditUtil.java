package com.inmobi.messaging.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.messaging.Message;

public class AuditUtil {
  static final byte[] magicBytes = { (byte) 0xAB, (byte) 0xCD,
      (byte) 0xEF };
  public static final String AUDIT_STREAM_TOPIC_NAME = "_audit";
  private static final byte versions[] = { 1 };
  private static final int currentVersion = 1;
  private static final Log LOG = LogFactory.getLog(AuditUtil.class);
  static final int HEADER_LENGTH = 16;
  private static final long BASE_TIME = 1356998400000l;
  public static final String DATE_FORMAT = "dd-MM-yyyy-HH:mm";

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

  public static ByteBuffer removeHeader(byte data[]) {
    if (isValidHeaders(data)) {
      return ByteBuffer.wrap(Arrays.copyOfRange(data, HEADER_LENGTH,
          data.length));
    } else
      return ByteBuffer.wrap(data);
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

}
