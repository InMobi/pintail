package com.inmobi.messaging.consumer.util;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MessageUtil {

  public static String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    str.append(index).append("Message");
    return str.toString();
  }

  public static void createMessageFile(String fileName, FileSystem fs, Path parent,
      int msgIndex) throws IOException {
    FSDataOutputStream out = fs.create(new Path(parent, fileName));
    for (int i = 0; i < 100; i++) {
      out.write(Base64.encodeBase64(constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }
    out.close();
    TestUtil.LOG.debug("Created data file:" + new Path(parent, fileName));
  }

}
