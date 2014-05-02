package com.inmobi.databus.readers;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class TestLocalStreamCollectorReader {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);
  private LocalStreamCollectorReader lreader;
  private ClusterUtil cluster;
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  private Path[] databusFiles = new Path[3];

  private String doesNotExist1 = TestUtil.files[0];
  private String doesNotExist2 = TestUtil.files[2];
  private String doesNotExist3 = TestUtil.files[7];
  Configuration conf;
  int consumerNumber;
  String fsUri;

  @BeforeTest
  public void setup() throws Exception {
    // initialize config
    consumerNumber = 1;
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, databusFiles, 3,
        TestUtil.getConfiguredRootDir());
    conf = cluster.getHadoopConf();
    fsUri = FileSystem.get(conf).getUri().toString();
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    CollectorReaderStatsExposer metrics = new
        CollectorReaderStatsExposer(testStream, "c1", partitionId.toString(),
            consumerNumber, fsUri);
    // Read from start
    lreader = new LocalStreamCollectorReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        TestUtil.getStreamsLocalDir(cluster, testStream), conf, 0L,
        metrics, null);
    lreader.build(CollectorStreamReader.getDateFromCollectorFile(files[0]));

    lreader.initFromStart();
    Assert.assertEquals(lreader.getCurrentFile(),
        databusFiles[0]);

    // Read from checkpoint with local stream file name
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            files[1]), 20));
    Assert.assertEquals(lreader.getCurrentFile(),
        databusFiles[1]);

    // Read from checkpoint with local stream file name, which does not exist
    // and is before the stream
    PartitionCheckpoint pck = new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist1), 20);
    lreader.initializeCurrentFile(pck);
    lreader.initFromNextHigher(pck.getFileName());
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[0]);

    // Read from checkpoint with local stream file name, which does not exist
    // with file timestamp after the stream
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist3), 20));
    Assert.assertNull(lreader.getCurrentFile());

    // Read from checkpoint with local stream file name, which does not exist
    // with file timestamp within the stream
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist2), 20));
    Assert.assertNull(lreader.getCurrentFile());

    //Read from startTime in local stream directory
    lreader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(files[1]));
    Assert.assertEquals(lreader.getCurrentFile(),
        databusFiles[1]);

    //Read from startTime in before the stream
    lreader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1));
    Assert.assertEquals(lreader.getCurrentFile(),
        databusFiles[0]);

    //Read from startTime within the stream
    lreader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2));
    Assert.assertEquals(lreader.getCurrentFile(),
        databusFiles[1]);

    //Read from startTime after stream
    lreader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3));
    Assert.assertNull(lreader.getCurrentFile());

  }

  private void readFile(int fileNum, int startIndex) throws Exception {
    int fileIndex = fileNum * 100 ;
    for (int i = startIndex; i < 100; i++) {
      Message line = lreader.readLine();
      Assert.assertNotNull(line);
      String msg = new String(line.getData().array());
      Assert.assertEquals(msg,
          MessageUtil.constructMessage(fileIndex + i));
    }
    Assert.assertEquals(lreader.getCurrentFile().getName(),
        databusFiles[fileNum].getName());
  }

  @Test
  public void testReadFromStart() throws Exception {
    CollectorReaderStatsExposer metrics = new
        CollectorReaderStatsExposer(testStream, "c1", partitionId.toString(),
            consumerNumber, fsUri);
    lreader = new LocalStreamCollectorReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        TestUtil.getStreamsLocalDir(cluster, testStream), conf,
        0L, metrics, null);
    lreader.build(CollectorStreamReader.getDateFromCollectorFile(files[0]));
    lreader.initFromStart();
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(0, 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 100);
    readFile(1, 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 200);
    readFile(2, 0);
    lreader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(metrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(metrics.getListOps() > 0);
    Assert.assertTrue(metrics.getOpenOps() == 0);
    Assert.assertTrue(metrics.getFileStatusOps() > 0);
    Assert.assertTrue(metrics.getExistsOps() > 0);
    Assert.assertTrue(metrics.getNumberRecordReaders() > 0);
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    CollectorReaderStatsExposer metrics = new 
        CollectorReaderStatsExposer(testStream, "c1", partitionId.toString(),
            consumerNumber, fsUri);
    lreader = new LocalStreamCollectorReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        TestUtil.getStreamsLocalDir(cluster, testStream), conf, 0L,
        metrics, null);
    PartitionCheckpoint pcp = new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            files[1]), 20);
    lreader.build(LocalStreamCollectorReader.getBuildTimestamp(testStream,
        collectorName, pcp));
    lreader.initializeCurrentFile(pcp);
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(1, 20);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 80);
    readFile(2, 0);
    lreader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 180);
    Assert.assertEquals(metrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(metrics.getListOps() > 0);
    Assert.assertTrue(metrics.getOpenOps() == 0);
    Assert.assertTrue(metrics.getFileStatusOps() > 0);
    Assert.assertTrue(metrics.getExistsOps() > 0);
    Assert.assertTrue(metrics.getNumberRecordReaders() > 0);
  }

  @Test
  public void testReadFromTimeStamp() throws Exception {
    CollectorReaderStatsExposer metrics = new
        CollectorReaderStatsExposer(testStream, "c1", partitionId.toString(),
            consumerNumber, fsUri);
    lreader = new LocalStreamCollectorReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        TestUtil.getStreamsLocalDir(cluster, testStream), conf, 0L,
        metrics, null);
    lreader.build(CollectorStreamReader.getDateFromCollectorFile(files[1]));
    lreader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(files[1]));
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(1, 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 100);
    readFile(2, 0);
    lreader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(metrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(metrics.getListOps() > 0);
    Assert.assertTrue(metrics.getOpenOps() == 0);
    Assert.assertTrue(metrics.getFileStatusOps() > 0);
    Assert.assertTrue(metrics.getExistsOps() > 0);
    Assert.assertTrue(metrics.getNumberRecordReaders() > 0);
  }

}
