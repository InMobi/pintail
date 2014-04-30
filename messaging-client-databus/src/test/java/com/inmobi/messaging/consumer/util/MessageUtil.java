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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

public class MessageUtil {

  public static String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    // message length should be more than 65
    str.append(index).append(
        "Message-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
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

  public static void createMessageSequenceFile(String fileName, FileSystem fs,
      Path parent, int msgIndex, Configuration conf) throws IOException {
    Path file = new Path(parent, fileName);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, file,
        IntWritable.class, Text.class, CompressionType.NONE);

    for (int i = 0; i < 100; i++) {
      writer.append(new IntWritable(i),
          new Text(constructMessage(msgIndex).getBytes()));
      msgIndex++;
    }
    writer.close();
    TestUtil.LOG.debug("Created sequence data file:" + file);
  }

  public static void createEmptySequenceFile(String fileName, FileSystem fs,
      Path parent, Configuration conf) throws IOException {
    Path file = new Path(parent, fileName);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, file,
        IntWritable.class, Text.class, CompressionType.NONE);
    writer.close();
    TestUtil.LOG.debug("Created empty sequence file:" + file);
  }

  public static Text getTextMessage(byte[] line) throws IOException {
    Text text = new Text();
    ByteArrayInputStream bais = new ByteArrayInputStream(line);
    text.readFields(new DataInputStream(bais));
    return text;
  }
}
