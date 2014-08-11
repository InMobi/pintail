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
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import com.inmobi.messaging.Message;

public class DatabusBytesWritableRecordReader implements RecordReader<LongWritable, BytesWritable> {

  private DatabusRecordReader databusReader;
  private Message messageValue;

  public DatabusBytesWritableRecordReader(JobConf job, InputSplit split) throws IOException {
    databusReader = new DatabusRecordReader(job, (FileSplit) split);
  }

  public LongWritable createKey() {
    return databusReader.createKey();
  }

  public BytesWritable createValue() {
    messageValue = databusReader.createValue();
    return new BytesWritable();
  }

  public long getPos() throws IOException {
    return databusReader.getPos();
  }

  public boolean next(LongWritable key, BytesWritable value) throws IOException {
    messageValue.clear();
    boolean ret = databusReader.next(key, messageValue);
    if (ret) {
      // get the byte array corresponding to the value read
      ByteBuffer buffer = messageValue.getData();
      byte[] data = new byte[buffer.remaining()];
      buffer.get(data);
      value.set(data, 0, data.length);
    }
    return ret;
  }

  public void close() throws IOException {
    databusReader.close();
  }

  public float getProgress() throws IOException {
    return databusReader.getProgress();
  }
}
