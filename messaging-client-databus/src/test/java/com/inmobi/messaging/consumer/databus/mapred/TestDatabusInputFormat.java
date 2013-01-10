package com.inmobi.messaging.consumer.databus.mapred;

import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.TestAbstractInputFormat;

public class TestDatabusInputFormat extends TestAbstractInputFormat {
  private static final Log LOG =
      LogFactory.getLog(TestDatabusInputFormat.class.getName());
  DatabusInputFormat databusInputFormat;

  @BeforeTest
  public void setUp() throws Exception {
    databusInputFormat = new DatabusInputFormat();
    rootDir = new Path("file:///tmp/test/databustestMapRed");
    super.setUp();
  }

  public static List<Message> readSplit(DatabusInputFormat format, 
      InputSplit split, 
      JobConf job, Reporter reporter) throws IOException {
    List<Message> result = new ArrayList<Message>();
    RecordReader<LongWritable, Message> reader =
        format.getRecordReader(split, job, reporter);
    LongWritable key = ((DatabusRecordReader)reader).createKey();
    Message value = ((DatabusRecordReader)reader).createValue();

    while (((DatabusRecordReader)reader).next(key, value)) {
      result.add(value);
      value = (Message) ((DatabusRecordReader)reader).createValue();
    }
    reader.close();
    return result;
  }

  protected void splitFile(int numSplits) throws Exception {
    InputSplit inputSplit[] = databusInputFormat.getSplits(defaultConf, 
        numSplits);
    LOG.info("number of splits : " + inputSplit.length);
    for (InputSplit split : inputSplit) {
      readMessages.addAll(readSplit(databusInputFormat, split, defaultConf, 
          reporter));
    }
  }

  @Test
  public void testDatabusInputFormat() throws Exception {
    FileInputFormat.setInputPaths(defaultConf, collectorDir);
    splitFile(5);
    assertMessages(100);
  }

  @Test
  protected void testGZFile() throws Exception {
    Path localstreamDir = new Path(cluster.getLocalFinalDestDirRoot(), 
        testStream);
    List<Path> minuteDirs = new ArrayList<Path>();
    listAllPaths(localstreamDir, minuteDirs);
    if (minuteDirs.size() > 0) {
      FileInputFormat.setInputPaths(defaultConf, minuteDirs.get(0).getParent());
      readMessages = new ArrayList<Message>();
      splitFile(1);
      System.out.println("size is "+ readMessages.size());
      assertMessages(0);
    }
  }

  @AfterTest
  public void cleanUp() throws Exception {
    fs.delete(rootDir, true);
  }
}