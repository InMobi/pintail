package com.inmobi.databus.partition;

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

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderHadoopStream extends TestAbstractClusterReader {
  static final Log LOG = LogFactory.getLog(TestPartitionReaderHadoopStream.class);
  @BeforeTest
  public void setup() throws Exception {
    consumerNumber = 1;
    // setup fs
    files = new String[] {HadoopUtil.files[1], HadoopUtil.files[3],
        HadoopUtil.files[5]};
    fs = FileSystem.getLocal(conf);
    streamDir = new Path(new Path(TestUtil.getConfiguredRootDir(),
        this.getClass().getSimpleName()), testStream).makeQualified(fs);
    HadoopUtil.setupHadoopCluster(conf, files, null, databusFiles, streamDir, false);
    inputFormatClass = SequenceFileInputFormat.class.getName();
    partitionMinList = new TreeSet<Integer>();
    for (int i = 0; i < 60; i++) {
      partitionMinList.add(i);
    }
    Map<Integer, PartitionCheckpoint> chkpoints = new TreeMap<Integer,
        PartitionCheckpoint>();
    partitionCheckpointList = new PartitionCheckpointList(chkpoints);
  }

  @AfterTest
  public void cleanup() throws IOException {
    LOG.debug("Cleaning up the dir: " + streamDir.getParent());
    fs.delete(streamDir.getParent(), true);
  }

  @Test
  public void testInitialize() throws Exception {
    super.testInitialize();
  }

  @Test
  public void testReadFromStart() throws Exception {
    super.testReadFromStart();
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    super.testReadFromCheckpoint();
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    super.testReadFromCheckpointWhichDoesNotExist();
  }

  @Test
  public void testReadFromStartTime() throws Exception {
    super.testReadFromStartTime();
  }

  @Test
  public void testReadFromStartTimeWithinStream() throws Exception {
    super.testReadFromStartTimeWithinStream();
  }


  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    super.testReadFromStartTimeBeforeStream();
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    super.testReadFromStartTimeAfterStream();
  }

  @Test
  public void testReadFromCheckpointWithSingleMinute() throws Exception {
    super.testReadFromCheckpointWithSingleMinute();
  }

  @Test
  public void testReadFromCheckpointMultipleMinutes() throws Exception {
    super.testReadFromCheckpointMultipleMinutes();
  }

  @Test
  public void testReadFromCheckpointSomeMinutes() throws Exception {
    super.testReadFromCheckpointSomeMinutes();
  }

  @Test
  public void testReadFromMultipleCompleteCheckpoints() throws Exception {
    super.testReadFromMultipleCompleteCheckpoints();
  }

  @Test
  public void testReadFromSingleCompleteCheckpoint() throws Exception {
    super.testReadFromSingleCompleteCheckpoint();
  }

  @Test
  public void testReadFromTwoCompleteCheckpoint() throws Exception {
    super.testReadFromTwoCompleteCheckpoint();
  }

  @Test
  public void testReadFromCheckpointsAndSomeNotExists() throws Exception {
    super.testReadFromCheckpointsAndSomeNotExists();
  }

  @Test
  public void testReadFromExistingCompletedCheckpointAndCheckpointNotExists()
      throws Exception {
    super.testReadFromExistingCompletedCheckpointAndCheckpointNotExists();
  }

  @Override
  Path getStreamsDir() {
    return streamDir;
  }

  @Override
  boolean isDatabusData() {
    return false;
  }

}
