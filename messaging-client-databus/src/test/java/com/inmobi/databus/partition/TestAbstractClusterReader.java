package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public abstract class TestAbstractClusterReader {
  protected static final String testStream = "testclient";
  protected static final String clusterName = "testCluster";
  protected PartitionId partitionId = new PartitionId(clusterName, null);
  protected LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  protected Cluster cluster;
  protected PartitionReader preader;

  protected String doesNotExist1 = TestUtil.files[0];
  protected String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  protected Path[] databusFiles = new Path[3];

  protected String doesNotExist2 = TestUtil.files[2];
  protected String doesNotExist3 = TestUtil.files[10];
  protected final String collectorName = "collector1";

  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  abstract boolean isLocal();

  public void testInitialize() throws Exception {
    // Read from start
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 1000, isLocal(), DataEncodingType.BASE64);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[0]);

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(
            DatabusStreamWaitingReader.getDatabusStreamFileName(
            collectorName, files[1]), 20),
            cluster, buffer, testStream, null, 1000, 1000, isLocal(),
            DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    // Read from checkpoint with local stream file name which does not exist
    // and is before the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        DatabusStreamWaitingReader.getDatabusStreamFileName(collectorName,
            doesNotExist1), 20),
            cluster, buffer, testStream, null, 1000, 1000, isLocal(),
            DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[0]);

    //Read from startTime in local stream directory, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        isLocal(), DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    //Read from startTime in local stream directory, with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        isLocal(), DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    //Read from startTime in local stream directory, with no timestamp file,
    // with no checkpoint
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, isLocal(), DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    //Read from startTime in local stream directory, with no timestamp file,
    //with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, isLocal(), DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    //Read from startTime beyond the stream
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, isLocal(), DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[0]);

    //Read from startTime after the stream
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3),
        1000, 1000, isLocal(), DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());

    //Read from startTime beyond the stream, with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1),
        1000, 1000, isLocal(), DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[0]);

    //Read from startTime after the stream, with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3),
        1000, 1000, isLocal(), DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());
  }

  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 1000, isLocal(), DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
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
        DatabusStreamWaitingReader.class.getName());
  }

  public void testReadFromCheckpoint() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream, null,
        1000, 1000, isLocal(), DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 20, 80, partitionId,
        buffer);
    TestUtil.assertBuffer(databusFiles[2].getName(), 3, 0, 100, partitionId,
        buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
  }

  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        DatabusStreamWaitingReader.getDatabusStreamFileName(collectorName,
            doesNotExist1),
        20), cluster, buffer, testStream, null, 1000, 1000,
        isLocal(), DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
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
        DatabusStreamWaitingReader.class.getName());
  }

  public void testReadFromCheckpointWhichDoesNotExist2() throws Exception {
    Throwable th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
          doesNotExist2, 20), cluster, buffer, testStream, null, 1000, 1000,
          isLocal(), DataEncodingType.BASE64, true);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

  public void testReadFromCheckpointWhichDoesNotExist3() throws Exception {
    Throwable th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
          doesNotExist3, 20), cluster, buffer, testStream, null, 1000, 1000,
          isLocal(), DataEncodingType.BASE64, true);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

  public void testReadFromStartTime() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        isLocal(), DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
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
        DatabusStreamWaitingReader.class.getName());
  }

  public void testReadFromStartTimeWithinStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, isLocal(), DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
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
        DatabusStreamWaitingReader.class.getName());
  }

  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, isLocal(), DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
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
        DatabusStreamWaitingReader.class.getName());
  }

  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        1000, isLocal(), DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
  }

}
