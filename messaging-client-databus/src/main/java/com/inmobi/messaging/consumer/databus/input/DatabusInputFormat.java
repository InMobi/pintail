package com.inmobi.messaging.consumer.databus.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.inmobi.messaging.Message;

public class DatabusInputFormat extends FileInputFormat<LongWritable, Message>{

  @Override
  public RecordReader<LongWritable, Message> createRecordReader(InputSplit arg0,
      TaskAttemptContext arg1) throws IOException, InterruptedException {
    return new DatabusRecordReader();
  }

}
