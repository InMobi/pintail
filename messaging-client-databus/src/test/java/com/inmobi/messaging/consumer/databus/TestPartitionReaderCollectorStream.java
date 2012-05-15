package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;

public class TestPartitionReaderCollectorStream {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  private Cluster cluster;
  private Path collectorDir;
  private PartitionReader preader;

  private String doesNotExist1 = TestUtil.files[0];
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[2],
      TestUtil.files[3]};

  private String doesNotExist2 = TestUtil.files[10];

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, 0);
    collectorDir = new Path(new Path(cluster.getDataDir(), testStream),
        collectorName);
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    // Read from start
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 10);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        files[0]));

    //Read from checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream, null, 1000, 10);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        files[1]));

    //Read from startTime without checkpoint
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 10);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        files[1])); 

    //Read from startTime with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 10);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        files[1])); 

  }

  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 10);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[0], 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[1], 2, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[2], 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream, null, 1000, 10);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[1], 2, 20, 80, partitionId, buffer);
    TestUtil.assertBuffer(files[2], 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        doesNotExist1, 20), cluster, buffer, testStream, null, 1000, 10);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTime() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 10);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[1], 2, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[2], 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000, 10);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[0], 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[1], 2, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[2], 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000, 10);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

}
