package com.inmobi.messaging.consumer.util;

import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.MessagingConsumerConfig;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.util.AuditUtil;

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
    return AuditUtil.removeHeader(data);
  }


}
