package com.inmobi.messaging.consumer.databus.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
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
  private Configuration conf;

  public DatabusRecordReader() {
    lineReader = new LineRecordReader();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    lineReader.initialize(split, context);
    conf = context.getConfiguration();
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
    byte[] line = text.getBytes();
    int length = text.getLength();
    byte[] msg = new byte[length];
    System.arraycopy(line, 0, msg, 0, length);
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
