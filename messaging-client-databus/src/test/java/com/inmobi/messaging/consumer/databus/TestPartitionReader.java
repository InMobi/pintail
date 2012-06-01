package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;

public class TestPartitionReader {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  private Cluster cluster;
  private Path collectorDir;
  private PartitionReader preader;
  private String[] files = new String[] {TestUtil.files[1],
      TestUtil.files[2], TestUtil.files[3], TestUtil.files[4],
      TestUtil.files[6], TestUtil.files[8]};

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files,
        new String[] {TestUtil.files[5], TestUtil.files[7], TestUtil.files[9]},
        3, true);
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
        1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[0]));

    // Read from checkpoint with collector file name
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream, null, 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[1]));

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(LocalStreamReader.getLocalStreamFileName(
            collectorName, files[1]), 20),
            cluster, buffer, testStream, null, 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[1]));

    // Read from checkpoint with collector file name
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[4], 20), cluster, buffer, testStream, null, 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        files[4]));    

    // Read from checkpoint with collector file name which does not exist
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        TestUtil.files[0], 40), cluster, buffer, testStream, null, 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        files[0]));

    // Read from checkpoint with local stream file name which does not exist
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName,
            TestUtil.files[0]), 20),
            cluster, buffer, testStream, null, 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        files[0]));

    // Read from checkpoint with collector file name which does not exist
    // but the collector file time stamp is after the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        TestUtil.files[10], 40), cluster, buffer, testStream, null, 1000);
    preader.initializeCurrentFile();
    Assert.assertNull(preader.getCurrentReader());

    // Read from checkpoint with local stream file name which does not exist
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName,
            TestUtil.files[10]), 20),
            cluster, buffer, testStream, null, 1000);
    preader.initializeCurrentFile();
    Assert.assertNull(preader.getCurrentReader());

    //Read from startTime in local stream directory 
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(null, -1), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[1]));

    //Read from startTime in collector dir
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[4]), 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir,
        files[4]));    

  }

  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[3], 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[4], 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
    preader.close();
  }

  @Test
  public void testReadFromCheckpointWithCollectorFileName() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream, null, 10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[1]), 2,  20, 80, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[3], 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[4], 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
    preader.close();
  }

  @Test
  public void testReadFromCheckpointWithLocalStreamFileName() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName, files[1]), 20),
        cluster, buffer, testStream, null, 10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[1]), 2,  20, 80, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[3], 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[4], 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
    preader.close();
  }

  @Test
  public void testReadFromCheckpointWithCollectorFile() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[4], 40), cluster, buffer, testStream, null, 10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[4], 5,  40, 60, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
    preader.close();
  }

  /**
   *  Disable this test if partition reader should not read from start of stream
   *  if check point does not exist.
   */
  @Test
  public void testReadFromCheckpointWithCollectorFileWhichDoesNotExist()
      throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        TestUtil.files[0], 40), cluster, buffer, testStream, null, 10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[3], 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[4], 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
    preader.close();
  }

  /**
   *  Disable this test if partition reader should not read from start of stream
   *  if check point does not exist.
   */
  @Test
  public void testReadFromCheckpointWithLocalStreamFileWhichDoesNotExist()
      throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName,
            TestUtil.files[0]), 20),
            cluster, buffer, testStream, null, 10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[3], 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[4], 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeInLocalStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[0], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[3], 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[4], 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeInCollectorStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[0], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[4]), 10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[4], 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[0]),
        10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(files[3], 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[4], 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[10]),
        10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }
}
