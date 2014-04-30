package com.inmobi.messaging.consumer.databus.mapred;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2014 InMobi
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
    rootDir = new Path("file:///",
        new Path(System.getProperty("test.root.dir"), "databustestMapRed"));
    super.setUp();
  }

  /**
   * read the the given input split.
   * @return List : List of read messages
   */
  private List<Message> readSplit(DatabusInputFormat format,
      InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    List<Message> result = new ArrayList<Message>();
    RecordReader<LongWritable, Message> reader =
        format.getRecordReader(split, job, reporter);
    LongWritable key = ((DatabusRecordReader) reader).createKey();
    Message value = ((DatabusRecordReader) reader).createValue();

    while (((DatabusRecordReader) reader).next(key, value)) {
      result.add(value);
      value = (Message) ((DatabusRecordReader) reader).createValue();
    }
    reader.close();
    return result;
  }

  protected void splitFile(int numSplits) throws Exception {
    InputSplit[] inputSplit = databusInputFormat.getSplits(defaultConf,
        numSplits);
    LOG.info("number of splits : " + inputSplit.length);
    for (InputSplit split : inputSplit) {
      readMessages.addAll(readSplit(databusInputFormat, split, defaultConf,
          reporter));
    }
  }

  /**
   * It reads the collector file (i.e. non compressed file) and assert on the
   * read messages
   */
  @Test
  public void testDatabusInputFormat() throws Exception {
    FileInputFormat.setInputPaths(defaultConf, collectorDir);
    splitFile(5);
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
      readMessages = new ArrayList<Message>();
      splitFile(1);
      assertMessages(0);
    }
  }

  @AfterTest
  public void cleanUp() throws Exception {
    LOG.debug("Cleaning up te dir: " + rootDir);
    fs.delete(rootDir, true);
  }
}