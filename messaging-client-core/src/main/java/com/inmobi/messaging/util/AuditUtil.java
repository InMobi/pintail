package com.inmobi.messaging.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.messaging.Message;

public class AuditUtil {
  private static final byte[] magicBytes = { (byte) 0xAB, (byte) 0xCD,
      (byte) 0xEF };
  public static final String AUDIT_STREAM_TOPIC_NAME = "_audit";
  private static final byte versions[] = { 1 };
  private static final int currentVersion = 1;
  private static final Log LOG = LogFactory.getLog(AuditUtil.class);
  private static final int HEADER_LENGTH = 16;
  private static final long BASE_TIME = 1356998400000l;
  public static final String DATE_FORMAT = "dd-MM-yyyy-HH:mm";

  public static void attachHeaders(Message m, Long timestamp) {
    byte[] b = m.getData().array();
    int messageSize = b.length;
    int totalSize = messageSize + 16;
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
    boolean isValidHeaders = true;
    if (data.length < 16) {
      LOG.debug("Total size of data in message is less than length of headers");
      isValidHeaders = false;
    }
    ByteBuffer buffer = ByteBuffer.wrap(data);
    boolean isVersionValid = false;
    byte versionFound = buffer.get();
    if (isValidHeaders) {
      for (byte version : versions) {
        if (versionFound == version) {
          isVersionValid = true;
          break;
        }
      }
      if (isVersionValid) {
        // compare all 3 magicBytes
        byte[] mBytesRead = new byte[3];
        buffer.get(mBytesRead);
        if (mBytesRead[0] != magicBytes[0] || mBytesRead[1] != magicBytes[1]
            || mBytesRead[2] != magicBytes[2]) {
          isValidHeaders = false;
          LOG.debug("Invalid magic bytes");
        }
        if (isValidHeaders) {
          long timestamp = buffer.getLong();
          if (timestamp < BASE_TIME) {
            isValidHeaders = false;
            LOG.debug("Invalid TimeStamp in headers [" + timestamp + "]");
          }
        }
        if (isValidHeaders) {
          int messageSize = buffer.getInt();
          if (isValidHeaders && data.length != HEADER_LENGTH + messageSize) {
            isValidHeaders = false;
            LOG.debug("Invalid size of message in headers;expected ["
                + (HEADER_LENGTH + messageSize) + "] found [" + data.length
                + "]");
          }
        }
      } else {
        isValidHeaders = false;
        LOG.debug("Invalid version in the headers;found version [ "
            + versionFound + "]");
      }

    }

    if (isValidHeaders) {
      return ByteBuffer.wrap(Arrays.copyOfRange(data, HEADER_LENGTH,
          data.length));
    } else
      return ByteBuffer.wrap(data);
  }

}
