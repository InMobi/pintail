package com.inmobi.databus.readers;

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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.databus.mapred.DatabusInputFormat;
import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class TestMergeStreamMultipleCollectors {

  private static final String testStream = "testclient";

  private String[] collectors = new String[] {"collector1", "collector2"};
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, null);
  private DatabusStreamWaitingReader reader;
  private ClusterUtil cluster;
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  Path[] databusFiles1 = new Path[3];
  Path[] databusFiles2 = new Path[3];
  Configuration conf;
  boolean encoded = true;
  Set<Integer> partitionMinList;
  PartitionCheckpointList partitionCheckpointList;
  Map<Integer, PartitionCheckpoint> chkPoints;
  int conusmerNumber;
  String fsUri;
  private String testRootDir;

  @BeforeTest
  public void setup() throws Exception {
    testRootDir = TestUtil.getConfiguredRootDir();
    conusmerNumber = 1;
    // initialize config
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectors[0]), files, null,
        databusFiles1, 0, 3, testRootDir);
    TestUtil.setUpFiles(cluster, collectors[1], files, null, databusFiles2, 0,
        3);
    conf = cluster.getHadoopConf();
    fsUri = FileSystem.get(conf).getUri().toString();
    partitionMinList = new TreeSet<Integer>();
    for (int i = 0; i < 60; i++) {
      partitionMinList.add(i);
    }
    chkPoints = new TreeMap<Integer, PartitionCheckpoint>();
    partitionCheckpointList = new PartitionCheckpointList(chkPoints);
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testReadFromStart() throws Exception {
    PartitionReaderStatsExposer metrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), conusmerNumber, fsUri);
    reader = new DatabusStreamWaitingReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()),
        TestUtil.getStreamsDir(cluster, testStream),
        DatabusInputFormat.class.getCanonicalName(),
        conf, 1000, metrics, false, partitionMinList, partitionCheckpointList,
        null);
    reader.build(CollectorStreamReader.getDateFromCollectorFile(files[0]));
    reader.initFromStart();
    Assert.assertNotNull(reader.getCurrentFile());
    reader.openStream();
    TestAbstractDatabusWaitingReader.readFile(reader, 0, 0, databusFiles1[0],
        encoded);
    TestAbstractDatabusWaitingReader.readFile(reader, 0, 0, databusFiles2[0],
        encoded);
    TestAbstractDatabusWaitingReader.readFile(reader, 1, 0, databusFiles1[1],
        encoded);
    TestAbstractDatabusWaitingReader.readFile(reader, 1, 0, databusFiles2[1],
        encoded);
    TestAbstractDatabusWaitingReader.readFile(reader, 2, 0, databusFiles1[2],
        encoded);
    TestAbstractDatabusWaitingReader.readFile(reader, 2, 0, databusFiles2[2],
        encoded);
    reader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 600);
    Assert.assertTrue(metrics.getListOps() > 0);
    Assert.assertTrue(metrics.getOpenOps() == 0);
    Assert.assertTrue(metrics.getFileStatusOps() > 0);
    Assert.assertTrue(metrics.getExistsOps() > 0);
    Assert.assertTrue(metrics.getNumberRecordReaders() > 0);
  }
}
