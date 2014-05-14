package com.inmobi.messaging.consumer.databus.mapreduce;

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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.util.DatabusUtil;

public class DatabusRecordReader extends RecordReader<LongWritable, Message> {

  private LineRecordReader lineReader;

  public DatabusRecordReader() {
    lineReader = new LineRecordReader();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    lineReader.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return lineReader.nextKeyValue();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return lineReader.getCurrentKey();
  }

  @Override
  public Message getCurrentValue() throws IOException, InterruptedException {
    Text text = lineReader.getCurrentValue();
    // get the byte array corresponding to the value read
    int length = text.getLength();
    byte[] msg = new byte[length];
    System.arraycopy(text.getBytes(), 0, msg, 0, length);
    return DatabusUtil.decodeMessage(msg);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return lineReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }
}
