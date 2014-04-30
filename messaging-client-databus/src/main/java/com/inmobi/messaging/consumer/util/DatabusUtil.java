package com.inmobi.messaging.consumer.util;

/*
 * #%L
 * messaging-client-databus
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

import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;

import com.inmobi.messaging.Message;
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
    case COLLECTOR:
      baseDir = new Path(databusRootDir, "data");
      break;
    case LOCAL:
      baseDir = new Path(databusRootDir, "streams_local");
      break;
    case MERGED:
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
    return AuditUtil.removeHeader(data);

  }

}
