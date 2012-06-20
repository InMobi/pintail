package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderLocalStream {
  private static final String testStream = "testclient";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, null);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  private Cluster cluster;
  private PartitionReader preader;
  private boolean isLocal = true;

  private String doesNotExist1 = TestUtil.files[0];
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  private Path[] databusFiles = new Path[3];

  private String doesNotExist2 = TestUtil.files[2];
  private String doesNotExist3 = TestUtil.files[10];
  private final String collectorName = "collector1";

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        databusFiles, 3);
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
        1000, isLocal);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[0]);

    //Read from checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream, null,
        1000, isLocal);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    //Read from startTime without checkpoint
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, isLocal);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]); 

    //Read from startTime with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(databusFiles[0].getName(), 20), cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[1]),
        1000, isLocal);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]); 

  }

  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, isLocal, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(databusFiles[0].getName(), 1, 0, 100, partitionId,
        buffer);
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 0, 100, partitionId,
        buffer);
    TestUtil.assertBuffer(databusFiles[2].getName(), 3, 0, 100, partitionId,
        buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream, null,
        1000, isLocal, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 20, 80, partitionId, buffer);
    TestUtil.assertBuffer(databusFiles[2].getName(), 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamReader.getDatabusStreamFileName(collectorName, doesNotExist1),
        20), cluster, buffer, testStream, null, 1000, isLocal,
        true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(databusFiles[0].getName(), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(databusFiles[2].getName(), 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist2() throws Exception {
    Throwable th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        doesNotExist2, 20), cluster, buffer, testStream, null, 1000, isLocal,
        true);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist3() throws Exception {
    Throwable th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        doesNotExist3, 20), cluster, buffer, testStream, null, 1000, isLocal,
        true);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

  @Test
  public void testReadFromStartTime() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, isLocal,
        true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 0, 100, partitionId,
        buffer);
    TestUtil.assertBuffer(databusFiles[2].getName(), 3, 0, 100, partitionId,
        buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
  }

  @Test
  public void testReadFromStartTimeWithinStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        isLocal, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 0, 100, partitionId,
        buffer);
    TestUtil.assertBuffer(databusFiles[2].getName(), 3, 0, 100, partitionId,
        buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        isLocal, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(databusFiles[0].getName(), 1, 0, 100, partitionId,
        buffer);
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 0, 100, partitionId,
        buffer);
    TestUtil.assertBuffer(databusFiles[2].getName(), 3, 0, 100, partitionId,
        buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        isLocal, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamReader.class.getName());
  }

}
