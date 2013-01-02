package com.inmobi.messaging.consumer.databus.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.inmobi.messaging.Message;

public class DatabusInputFormat extends FileInputFormat<LongWritable, Message>{

  @Override
  public RecordReader<LongWritable, Message> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new DatabusRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    CompressionCodec codec = 
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    return codec == null;
  }

}
