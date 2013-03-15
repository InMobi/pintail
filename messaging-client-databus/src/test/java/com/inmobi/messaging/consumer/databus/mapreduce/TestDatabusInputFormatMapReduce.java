package com.inmobi.messaging.consumer.databus.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.TestAbstractInputFormat;

public class TestDatabusInputFormatMapReduce extends TestAbstractInputFormat {
  private static final Log LOG =
      LogFactory.getLog(TestDatabusInputFormatMapReduce.class.getName());
  DatabusInputFormat databusInputFormat;

  @BeforeTest
  public void setUp() throws Exception {
    databusInputFormat = new DatabusInputFormat();
    rootDir = new Path("file:///tmp/test/databustestMapRduce");
    taskId = new TaskAttemptID("jt", 0, true, 0, 0);
    super.setUp();
  }

  /**
   * read the the given split.
   * @return List : List of read messages
   */
  private List<Message> readSplit(DatabusInputFormat format, 
      org.apache.hadoop.mapreduce.InputSplit split, 
      JobConf job) throws IOException,
      InterruptedException {
    List<Message> result = new ArrayList<Message>();
    RecordReader<LongWritable, Message> reader =
        format.createRecordReader((org.apache.hadoop.mapreduce.InputSplit) split,
            context);
    ((DatabusRecordReader)reader).initialize(split, context);
    while (reader.nextKeyValue()) {
      result.add(reader.getCurrentValue());
    }
    reader.close();
    return result;
  }

  public void splitFile(int numSplits, Path inputPath) throws Exception {
    if (databusInputFormat.isSplitable(context, inputPath)) {
      List<org.apache.hadoop.mapreduce.InputSplit> inputSplit = getInputSplits();
      for (org.apache.hadoop.mapreduce.InputSplit split : inputSplit) {
        readMessages.addAll(readSplit(databusInputFormat, split, defaultConf));
      }
    } else {
      LOG.info("not splittable " + inputPath);
      List<org.apache.hadoop.mapreduce.InputSplit> inputSplit = getInputSplits();
      readMessages.addAll(readSplit(databusInputFormat, inputSplit.get(0), 
          defaultConf));
    }
  }

  protected List<org.apache.hadoop.mapreduce.InputSplit> getInputSplits()
      throws IOException {
    List<org.apache.hadoop.mapreduce.InputSplit> inputSplit = 
        databusInputFormat.getSplits(context);
    return inputSplit;
  }

  /**
   * It reads the collector file (i.e. non compressed file) and assert on the 
   * read messages
   */
  @Test
  public void testDatabusInputFormatMapReduce() throws Exception {
    FileInputFormat.setInputPaths(defaultConf, collectorDir);
    context = new TaskAttemptContext(defaultConf, taskId);
    List<Path> collectorFilePaths = new ArrayList<Path>();
    listAllPaths(collectorDir, collectorFilePaths);

    if (collectorFilePaths.size() > 0) {
      splitFile(5, collectorFilePaths.get(0));
    }
    assertMessages(100);
  }

  /**
   * It reads the local stream file(i.e. compressed file) and assert on the 
   * read messages
   */
  @Test
  protected void testGZFile() throws Exception {
    Path localstreamDir = new Path(cluster.getLocalFinalDestDirRoot(), 
        testStream);
    List<Path> minuteDirs = new ArrayList<Path>();
    listAllPaths(localstreamDir, minuteDirs);
    if (minuteDirs.size() > 0) {
      FileInputFormat.setInputPaths(defaultConf, minuteDirs.get(0).getParent());
      context = new TaskAttemptContext(defaultConf, taskId);
      readMessages = new ArrayList<Message>();
      splitFile(1, minuteDirs.get(0));
      LOG.info("number msgs read from gz files  "+ readMessages.size());
      assertMessages(0);
    }
  }

  @AfterTest
  public void cleanUp() throws Exception {
    LOG.debug("Cleaning up the dir: " + rootDir);
    fs.delete(rootDir, true);
  }
}
