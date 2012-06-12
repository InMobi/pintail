package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;

public class TestPartitionReaderLocalStream {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  private Cluster cluster;
  private PartitionReader preader;
  private String[] files = new String[] {TestUtil.files[1],
      TestUtil.files[2], TestUtil.files[4]};
  private String doesNotExist1 = TestUtil.files[0];
  private String doesNotExist2 = TestUtil.files[3];
  private String doesNotExist3 = TestUtil.files[7];

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, 3);
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    // Read from starttime of stream
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[0]));

    // Read from checkpoint with collector file name, but file in local stream
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

    // Read from checkpoint with collector file name which does not exist
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        doesNotExist1, 40), cluster, buffer, testStream, null, 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        files[0]));

    // Read from checkpoint with local stream file name which does not exist
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName,
            doesNotExist1), 20),
            cluster, buffer, testStream, null, 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        files[0]));

    //Read from startTime in local stream directory, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[1]));

    //Read from startTime in local stream directory, with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[1]));

    //Read from startTime in local stream directory, with no timestamp file,
    // with no checkpoint
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[2]));

    //Read from startTime in local stream directory, with no timestamp file,
    //with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[2]));

    //Read from startTime beyond the stream
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[0]));

    //Read from startTime after the stream
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3),
        1000, true);
    preader.initializeCurrentFile();
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());

    //Read from startTime beyond the stream, with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[0]));

    //Read from startTime after the stream, with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3),
        1000, true);
    preader.initializeCurrentFile();
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());
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
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
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
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
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
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
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
        doesNotExist1, 40), cluster, buffer, testStream, null, 10, true);
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
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
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
            doesNotExist1), 20),
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
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
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
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeInLocalStream2() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[0], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1),
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
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        files[1], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3),
        10, true);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
  }
}
