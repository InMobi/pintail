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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.databus.mapred.DatabusInputFormat;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderWaitingMergeStream
    extends TestAbstractWaitingClusterReader {

  @BeforeMethod
  public void setup() throws Exception {
    files = new String[] {TestUtil.files[1],
        TestUtil.files[3], TestUtil.files[5]};
    newFiles = new String[] {TestUtil.files[6],
        TestUtil.files[7], TestUtil.files[8] };
    inputFormatClass = DatabusInputFormat.class.getName();
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        databusFiles, 0, 3, TestUtil.getConfiguredRootDir());
    conf = cluster.getHadoopConf();
    fs = FileSystem.get(conf);
    streamDir = DatabusUtil.getStreamDir(StreamType.MERGED,
        new Path(cluster.getRootDir()), testStream);
    partitionMinList = new HashSet<Integer>();
    for (int i = 0; i < 60; i++) {
      partitionMinList.add(i);
    }
    Map<Integer, PartitionCheckpoint> list = new
        HashMap<Integer, PartitionCheckpoint>();
    partitionCheckpointlist = new PartitionCheckpointList(list);
  }

  void setupFiles(String[] files, Path[] newDatabusFiles) throws Exception {
    TestUtil.setUpFiles(cluster, collectorName, files, null, newDatabusFiles,
        0, files.length);
  }

  @AfterMethod
  public void cleanup() throws IOException {
    super.cleanup();
  }

  @Test
  public void testReadFromStart() throws Exception {
    super.testReadFromStart();
  }

  @Test
  public void testReadFromStartOfStream() throws Exception {
    super.testReadFromStartOfStream();
  }

  @Override
  boolean isDatabusData() {
    return true;
  }
}
