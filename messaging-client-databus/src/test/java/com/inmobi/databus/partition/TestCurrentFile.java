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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.MiniClusterUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class TestCurrentFile {
  private static final String testStream = "testclient";
  private static final String collectorName = "collector1";
  private static final String clusterName = "testDFSCluster";

  private LinkedBlockingQueue<QueueEntry> buffer =
      new LinkedBlockingQueue<QueueEntry>(1000);

  private FileSystem fs;
  private ClusterUtil cluster;
  private Path collectorDir;
  private int msgIndex = 300;
  private PartitionReader preader;
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private String currentScribeFile = TestUtil.files[3];
  Configuration conf = new Configuration();
  private Path streamsLocalDir;
  int consumerNumber;
  String fsUri;


  private void writeMessages(FSDataOutputStream out, int num)
      throws IOException {
    for (int i = 0; i < num; i++) {
      out.write(Base64.encodeBase64(
          MessageUtil.constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }
    out.sync();
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
    MiniClusterUtil.shutdownDFSCluster();
  }

  @BeforeTest
  public void setup() throws Exception {
    consumerNumber = 1;
    cluster = TestUtil.setupDFSCluster(this.getClass().getSimpleName(),
        testStream, partitionId,
        MiniClusterUtil.getDFSCluster(conf).getFileSystem().getUri().toString(),
        null, null, 0, TestUtil.getConfiguredRootDir());
    collectorDir = DatabusUtil.getCollectorStreamDir(
        new Path(cluster.getRootDir()), testStream,
        collectorName);
    streamsLocalDir = DatabusUtil.getStreamDir(StreamType.LOCAL,
        new Path(cluster.getRootDir()), testStream);
    fs = FileSystem.get(cluster.getHadoopConf());
    fsUri = fs.getUri().toString();
  }

  @Test
  public void testReadFromCurrentScribeFile() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(currentScribeFile), 1000,
        1000, prMetrics, null);
    preader.start(null);
    Assert.assertTrue(buffer.isEmpty());
    FSDataOutputStream out = fs.create(
        new Path(collectorDir, currentScribeFile));
    writeMessages(out, 10);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(
        currentScribeFile), 4, 0, 10, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    writeMessages(out, 20);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(
        currentScribeFile), 4, 10, 20, partitionId, buffer, true, null);
    writeMessages(out, 20);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(
        currentScribeFile), 4, 30, 20, partitionId, buffer, true, null);
    writeMessages(out, 50);
    out.close();
    TestUtil.setUpEmptyFiles(fs, collectorDir, TestUtil.files[5]);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(
        currentScribeFile), 4, 50, 50, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    preader.join();
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 100);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 100);
    Assert.assertTrue(prMetrics.getWaitTimeInSameFile() > 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

}
