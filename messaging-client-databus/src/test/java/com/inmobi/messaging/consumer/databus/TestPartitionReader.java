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

  private String doesNotExist1 =  testStream + "-2012-05-02-14-24_00000";
  private String file1 =  testStream + "-2012-05-02-14-26_00000";
  private String file2 =  testStream + "-2012-05-02-14-27_00000";
  private String file3 =  testStream + "-2012-05-02-14-28_00000";
  private String file4 =  testStream + "-2012-05-02-14-29_00000";
  private String file41 = testStream + "-2012-05-02-14-30_00000";
  private String file5 =  testStream + "-2012-05-02-14-31_00000";
  private String file51 = testStream + "-2012-05-02-14-32_00000";
  private String file6 =  testStream + "-2012-05-02-14-33_00000";
  private String file61 = testStream + "-2012-05-02-14-34_00000";
  private String doesNotExist2 =  testStream + "-2012-05-02-14-35_00000";

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getName(), testStream,
        partitionId, new String[] {file1, file2, file3, file4, file5, file6},
        new String[] {file41, file51, file61}, 3);
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
    Assert.assertEquals(preader.getCurrentFile(),
      new Path(CollectorStreamReader.getDateDir(cluster, testStream, file1),
        LocalStreamReader.getLocalStreamFileName(collectorName, file1)));

    // Read from checkpoint with collector file name
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      file2, 20), cluster, buffer, testStream, null, 1000);
    Assert.assertEquals(preader.getCurrentFile(),
      new Path(CollectorStreamReader.getDateDir(cluster, testStream, file2),
        LocalStreamReader.getLocalStreamFileName(collectorName, file2)));

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
      new PartitionCheckpoint(LocalStreamReader.getLocalStreamFileName(
        collectorName, file2), 20),
        cluster, buffer, testStream, null, 1000);
    Assert.assertEquals(preader.getCurrentFile(),
      new Path(CollectorStreamReader.getDateDir(cluster, testStream, file2),
        LocalStreamReader.getLocalStreamFileName(collectorName, file2)));

    // Read from checkpoint with collector file name
    preader = new PartitionReader(partitionId,
      new PartitionCheckpoint(file5, 20), cluster, buffer, testStream, null, 1000);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir, file5));    

    //Read from startTime in local stream directory 
    preader = new PartitionReader(partitionId,
      new PartitionCheckpoint(null, -1), cluster, buffer, testStream,
      CollectorStreamReader.getDateFromFile(file2), 1000);
    Assert.assertEquals(preader.getCurrentFile(),
      new Path(CollectorStreamReader.getDateDir(cluster, testStream, file2),
        LocalStreamReader.getLocalStreamFileName(collectorName, file2)));

    //Read from startTime in collector dir
    preader = new PartitionReader(partitionId,
      new PartitionCheckpoint(file1, 10), cluster, buffer, testStream,
      CollectorStreamReader.getDateFromFile(file5), 1000);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir, file5));    

  }

  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      null, -1), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file1), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file2), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file3), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file4, 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file5, 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file6, 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWithCollectorFileName() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      file2, 20), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
      collectorName, file2), 2,  20, 80, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
      collectorName, file3), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file4, 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file5, 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file6, 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWithLocalStreamFileName() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      LocalStreamReader.getLocalStreamFileName(collectorName, file2), 20),
      cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
      collectorName, file2), 2,  20, 80, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
      collectorName, file3), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file4, 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file5, 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file6, 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWithCollectorFile() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      file5, 40), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file5, 5,  40, 60, partitionId, buffer);
    TestUtil.assertBuffer(file6, 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWithCollectorFileWhichDoesNotExist()
    throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      doesNotExist2, 40), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file1), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file2), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file3), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file4, 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file5, 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file6, 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWithLocalStreamFileWhichDoesNotExist()
    throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      LocalStreamReader.getLocalStreamFileName(collectorName,
        doesNotExist1), 20),
        cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file1), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file2), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file3), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file4, 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file5, 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file6, 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeInLocalStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      file1, 20), cluster, buffer, testStream,
      CollectorStreamReader.getDateFromFile(file2), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
      collectorName, file2), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
      collectorName, file3), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file4, 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file5, 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file6, 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeInCollectorStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      file1, 20), cluster, buffer, testStream,
      CollectorStreamReader.getDateFromFile(file5), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file5, 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file6, 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      file2, 20), cluster, buffer, testStream,
      CollectorStreamReader.getDateFromFile(doesNotExist1), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file1), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file2), 2,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
      file3), 3,  0, 100, partitionId, buffer);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
      CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(file4, 4,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file5, 5,  0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file6, 6,  0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
      file2, 20), cluster, buffer, testStream,
      CollectorStreamReader.getDateFromFile(doesNotExist2), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }
}
