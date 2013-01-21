package com.inmobi.messaging.consumer.databus.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileInputFormat;

import com.inmobi.messaging.Message;

public class DatabusInputFormat extends FileInputFormat<LongWritable, Message>{

  @Override
  public RecordReader<LongWritable, Message> getRecordReader(InputSplit split,
      JobConf job,
      Reporter reporter)
          throws IOException {
    reporter.setStatus(split.toString());
    return new DatabusRecordReader(job, split);
  }

}
