package com.inmobi.messaging.consumer.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.MessagingConsumerConfig;
import com.inmobi.messaging.consumer.databus.StreamType;

public class DatabusUtil {
  private static final Log LOG = LogFactory.getLog(DatabusUtil.class);
  private static final byte[] magicBytes = {(byte)0xAB,(byte)0xCD,(byte)0xEF};
  private static final byte[] versions = { 1 };
  private static final int HEADER_LENGTH = 16;
  public static Path getStreamDir(StreamType streamType, Path databusRootDir,
      String streamName) {
    return new Path(getBaseDir(streamType, databusRootDir), streamName);
  }

  public static Path getCollectorStreamDir(Path databusRootDir,
      String streamName, String collectorName) {
    return new Path(getStreamDir(StreamType.COLLECTOR, databusRootDir,
        streamName), collectorName);
  }

  public static Path getBaseDir(StreamType streamType, Path databusRootDir) {
    Path baseDir;
    switch (streamType) {
    case COLLECTOR :
      baseDir = new Path(databusRootDir, "data");
      break;
    case LOCAL :
      baseDir = new Path(databusRootDir, "streams_local");
      break;
    case MERGED :
      baseDir = new Path(databusRootDir, "streams");
      break;
    default:
      throw new IllegalArgumentException("Invalid type" + streamType);
    }
    return baseDir;
  }

  public static Message decodeMessage(byte[] line, Configuration conf) {
    return new Message(decodeByteBuffer(line, conf));
  }

  public static void decodeMessage(byte[] line, Configuration conf,
      Message msg) {
    msg.set(decodeByteBuffer(line, conf));
  }

  private static ByteBuffer decodeByteBuffer(byte[] line, Configuration conf) {
    DataEncodingType dataEncoding = DataEncodingType.valueOf(
        conf.get(MessagingConsumerConfig.dataEncodingConfg,
            MessagingConsumerConfig.DEFAULT_DATA_ENCODING));
    byte[] data;
    if (dataEncoding.equals(DataEncodingType.BASE64)) {
      data = Base64.decodeBase64(line);
    } else {
      data = line;
    }
    return removeHeader(data);
  }

  private static ByteBuffer removeHeader(byte data[]) {
  boolean isValidHeaders = true;
  if (data.length < 16) {
    LOG.debug("Total size of data in message is less than length of headers");
    isValidHeaders = false;
  }
  ByteBuffer buffer = ByteBuffer.wrap(data);
  boolean isVersionValid = false;
  if (isValidHeaders) {
    for (byte version : versions) {
      if (buffer.get() == version) {
        isVersionValid = true;
        break;
      }
    }
    if (isVersionValid) {
      // compare all 3 magicBytes
      byte[] mBytesRead = new byte[3];
      buffer.get(mBytesRead);
      if (mBytesRead[0] != magicBytes[0] || mBytesRead[1] != magicBytes[1]
          || mBytesRead[2] != magicBytes[2])
        isValidHeaders = false;
    } else {
      LOG.debug("Invalid version in the headers");
    }
  }
  // TODO add validation for timestamp
  long timestamp = buffer.getLong();

  int messageSize = buffer.getInt();
    if (isValidHeaders && data.length != HEADER_LENGTH + messageSize) {
    isValidHeaders = false;
    LOG.debug("Invalid size of messag in headers");
  }

  if (isValidHeaders) {
      return ByteBuffer.wrap(Arrays.copyOfRange(data, HEADER_LENGTH,
          data.length));
  }
else
      return ByteBuffer.wrap(data);
}
}
