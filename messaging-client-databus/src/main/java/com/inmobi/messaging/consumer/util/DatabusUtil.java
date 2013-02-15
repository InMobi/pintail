package com.inmobi.messaging.consumer.util;

import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.StreamType;

public class DatabusUtil {

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

  public static Message decodeMessage(byte[] line) {
    return new Message(decodeByteBuffer(line));
  }

  public static void decodeMessage(byte[] line, Message msg) {
    msg.set(decodeByteBuffer(line));
  }

  private static ByteBuffer decodeByteBuffer(byte[] line) {
    byte[] data = Base64.decodeBase64(line);
    return ByteBuffer.wrap(data);
  }
}
