package com.inmobi.messaging.consumer.databus.mapred;

/*
 * #%L
 * messaging-client-databus
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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.LineRecordReader;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.util.DatabusUtil;

public class DatabusRecordReader implements RecordReader<LongWritable, Message> {

  private LineRecordReader lineReader;
  private Text textValue;

  public DatabusRecordReader(JobConf job, InputSplit split) throws IOException {
    lineReader = new LineRecordReader(job, (FileSplit) split);
  }

  public LongWritable createKey() {
    return lineReader.createKey();
  }

  public Message createValue() {
    textValue = lineReader.createValue();
    return new Message(new byte[0]);
  }

  public long getPos() throws IOException {
    return lineReader.getPos();
  }

  public boolean next(LongWritable key, Message value) throws IOException {
    textValue.clear();
    boolean ret = lineReader.next(key, this.textValue);
    if (ret) {
      // get the byte array corresponding to the value read
      int length = textValue.getLength();
      byte[] msg = new byte[length];
      System.arraycopy(textValue.getBytes(), 0, msg, 0, length);
      //decode Base 64
      DatabusUtil.decodeMessage(msg, value);
    }
    return ret;
  }

  public void close() throws IOException {
    lineReader.close();
  }

  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }
}
