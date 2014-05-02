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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.databus.mapred.DatabusInputFormat;
import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderLocalStream extends TestAbstractClusterReader {
  static final Log LOG = LogFactory.getLog(TestPartitionReaderLocalStream.class);
  ClusterUtil cluster;
  @BeforeTest
  public void setup() throws Exception {
    consumerNumber = 1;
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        databusFiles, 3, TestUtil.getConfiguredRootDir());
    fs = FileSystem.get(cluster.getHadoopConf());
    streamDir = TestUtil.getStreamsLocalDir(cluster,
        testStream);
    inputFormatClass = DatabusInputFormat.class.getName();
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
    LOG.debug("Cleaning up the dir: " + cluster.getRootDir());
    fs.delete(new Path(cluster.getRootDir()), true);
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

  @Override
  Path getStreamsDir() {
    return streamDir;
  }

  @Override
  boolean isDatabusData() {
    return true;
  }

}
