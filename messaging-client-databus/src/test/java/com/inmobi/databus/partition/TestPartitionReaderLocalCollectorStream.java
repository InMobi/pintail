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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.EOFMessage;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class TestPartitionReaderLocalCollectorStream {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer =
      new LinkedBlockingQueue<QueueEntry>(1000);
  private ClusterUtil cluster;
  private PartitionReader preader;
  private String[] files = new String[] {TestUtil.files[1],
      TestUtil.files[2], TestUtil.files[4]};
  private Path[] databusFiles = new Path[3];
  private String doesNotExist1 = TestUtil.files[0];
  private String doesNotExist2 = TestUtil.files[3];
  private String doesNotExist3 = TestUtil.files[7];
  private Path collectorDir;
  private Path streamsLocalDir;
  private Configuration conf = new Configuration();
  private FileSystem fs;
  int consumerNumber;
  String fsUri;

  @BeforeTest
  public void setup() throws Exception {
  	consumerNumber = 1;
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, databusFiles, 3,
        TestUtil.getConfiguredRootDir());
    collectorDir = DatabusUtil.getCollectorStreamDir(
        new Path(cluster.getRootDir()), testStream,
        collectorName);
    streamsLocalDir = DatabusUtil.getStreamDir(StreamType.LOCAL,
        new Path(cluster.getRootDir()), testStream);
    fs = FileSystem.get(cluster.getHadoopConf());
    fsUri = fs.getUri().toString();
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    // Read from starttime of stream
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 1000, 1000,
        prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    // Read from checkpoint with collector file name, but file in local stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream, null,
        1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(LocalStreamCollectorReader.getDatabusStreamFile(
            collectorName, files[1]), 20),
            conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
            1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    // Read from checkpoint with collector file name which does not exist
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream,
        null, 1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    // Read from checkpoint with local stream file name which does not exist
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist1), 20),
            conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
            1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    //Read from startTime in local stream directory
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    //Read from startTime in local stream directory, with no timestamp file
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[2].getName());

    //Read from startTime beyond the stream
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    //Read from startTime after the stream
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        1000, prMetrics, true, null);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());

    //Read from checkpoint which does not exist, with startTime beyond the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), conf, fs,
        collectorDir,
        streamsLocalDir, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());
  }

  @Test
  public void testReadFromStart() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 10, 1000,
        prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
  }

  @Test
  public void testReadFromCheckpointWithCollectorFileName() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream, null,
        10, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  20, 80, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    preader.join();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 180);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 180);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
  }

  @Test
  public void testReadFromCheckpointWithLocalStreamFileName() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            files[1]), 20),
        conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
        10, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  20, 80, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    preader.join();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 180);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 180);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
  }

  /**
   *  Disable this test if partition reader should not read from start of stream
   *  if check point does not exist.
   */
  @Test
  public void testReadFromCheckpointWithCollectorFileWhichDoesNotExist()
      throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream, null,
        10, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    preader.join();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
  }

  /**
   *  Disable this test if partition reader should not read from start of stream
   *  if check point does not exist.
   */
  @Test
  public void testReadFromCheckpointWithLocalStreamFileWhichDoesNotExist()
      throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist1), 20),
            conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
            10, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    preader.join();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
  }


  @Test
  public void testReadFromStartTimeInLocalStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 10, 1000,
        prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    preader.join();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 200);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
  }

  @Test
  public void testReadFromStartTimeInLocalStream2() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 10, 1000,
        prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    preader.join();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 100);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 100);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 10, 1000,
        prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
    preader.join();
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 10, 1000,
        prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 0);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
  }
}
