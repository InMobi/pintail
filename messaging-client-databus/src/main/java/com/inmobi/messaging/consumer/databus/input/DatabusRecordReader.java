package com.inmobi.messaging.consumer.databus.input;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.inmobi.messaging.Message;

public class DatabusRecordReader extends RecordReader<LongWritable, Message> {

  private LineRecordReader lineReader;

  @Override
  public void close() throws IOException {
    lineReader.close();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException,
      InterruptedException {
    return lineReader.getCurrentKey();
  }

  @Override
  public Message getCurrentValue() throws IOException, InterruptedException {
    byte[] line = lineReader.getCurrentValue().getBytes();
    
    //TODO:strip headers if any
    
    //decode Base 64
    byte[] data = Base64.decodeBase64(line);

    return new Message(data);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return lineReader.getProgress();
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    lineReader.initialize(genericSplit, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return lineReader.nextKeyValue();
  }

  
}