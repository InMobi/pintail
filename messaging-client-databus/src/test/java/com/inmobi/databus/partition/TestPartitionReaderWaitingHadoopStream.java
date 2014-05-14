package com.inmobi.databus.partition;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderWaitingHadoopStream extends
    TestAbstractWaitingClusterReader {
  static final Log LOG = LogFactory.getLog(
      TestPartitionReaderWaitingHadoopStream.class);
  @BeforeMethod
  public void setup() throws Exception {
    consumerNumber = 1;
    conf = new Configuration();
    files = new String[] {HadoopUtil.files[1],
        HadoopUtil.files[3], HadoopUtil.files[5]};
    newFiles = new String[] {HadoopUtil.files[6],
        HadoopUtil.files[7], HadoopUtil.files[8] };
    // setup fs
    fs = FileSystem.getLocal(conf);
    streamDir = new Path(new Path(TestUtil.getConfiguredRootDir(),
        this.getClass().getSimpleName()), testStream).makeQualified(fs);
    HadoopUtil.setupHadoopCluster(conf, files, null, databusFiles, streamDir, false);
    inputFormatClass = SequenceFileInputFormat.class.getName();
    partitionMinList = new HashSet<Integer>();
    for (int i = 0; i < 60; i++) {
      partitionMinList.add(i);
    }
    Map<Integer, PartitionCheckpoint> list = new
        HashMap<Integer, PartitionCheckpoint>();
    partitionCheckpointlist = new PartitionCheckpointList(list);
  }

  @AfterMethod
  public void cleanup() throws IOException {
    LOG.debug("Cleaning up the dir: " + streamDir.getParent());
    fs.delete(streamDir.getParent(), true);
  }

  @Override
  void setupFiles(String[] files, Path[] newDatabusFiles) throws Exception {
    HadoopUtil.setUpHadoopFiles(streamDir, conf, files, null, newDatabusFiles);
  }

  @Test
  public void testReadFromStart() throws Exception {
    super.testReadFromStart();
  }

  @Test
  public void testReadFormStartOfStream() throws Exception {
    super.testReadFromStartOfStream();
  }

  @Override
  boolean isDatabusData() {
    return false;
  }
}
