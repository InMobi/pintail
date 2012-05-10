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

  private String doesNotExist1 = testStream + "-2012-05-02-14-24_00000";
  private String file1 = testStream + "-2012-05-02-14-26_00000";
  private String file2 = testStream + "-2012-05-02-14-27_00000";
  private String file3 = testStream + "-2012-05-02-14-28_00000";
  private String doesNotExist2 = testStream + "-2012-05-02-14-30_00000";

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, new String[] {file1, file2, file3}, null, 0);
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
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        null, -1), cluster, buffer, testStream, null, 1000);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        file1));

    //Read from checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file2, 20), cluster, buffer, testStream, null, 1000);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        file2));

    //Read from startTime without checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(null, -1), cluster, buffer, testStream,
        TestUtil.getDateFromCollectorFile(file2), 1000);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        file2)); 

    //Read from startTime with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(file1, 20), cluster, buffer, testStream,
        TestUtil.getDateFromCollectorFile(file2), 1000);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        file2)); 

  }

  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        null, -1), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file1, 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file2, 2, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file3, 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file2, 20), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file2, 2, 20, 80, partitionId, buffer);
    TestUtil.assertBuffer(file3, 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        doesNotExist1, 20), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTime() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file2, 20), cluster, buffer, testStream,
        TestUtil.getDateFromCollectorFile(file2), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file2, 2, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file3, 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file2, 20), cluster, buffer, testStream,
        TestUtil.getDateFromCollectorFile(doesNotExist1), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file1, 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file2, 2, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file3, 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file2, 20), cluster, buffer, testStream,
        TestUtil.getDateFromCollectorFile(doesNotExist2), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

}
