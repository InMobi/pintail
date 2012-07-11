package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderCollectorStream {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  private Cluster cluster;
  private PartitionReader preader;

  private String doesNotExist1 = TestUtil.files[0];
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};

  private String doesNotExist2 = TestUtil.files[2];
  private String doesNotExist3 = TestUtil.files[10];

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, 0);
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
        1000, 1000, false, DataEncodingType.BASE64);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer,
        testStream, null, 1000, 1000, false,
        DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);

    //Read from startTime without checkpoint
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]); 

    //Read from startTime with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 20), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]); 

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(
            LocalStreamCollectorReader.getDatabusStreamFile(
            collectorName, doesNotExist1), 20),
            cluster, buffer, testStream, null, 1000, 1000, false,
            DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), 
        files[0]);

    // Read from checkpoint with collector file name and file in collector
    // stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer,
        testStream, null, 1000, 1000, false,
        DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);    

    // Read from checkpoint with collector file name which does not exist
    // and is before the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), cluster,
        buffer, testStream, null, 1000, 1000,
        false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    // Read from checkpoint with local stream file name which does not exist
    // and is before the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist1), 20),
            cluster, buffer, testStream, null, 1000, 1000, false,
            DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    // Read from checkpoint with collector file name which does not exist
    // but the collector file time stamp is after the stream
    Throwable th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
          CollectorStreamReader.getCollectorFile(doesNotExist3), 40), cluster,
          buffer, testStream, null, 1000,
          1000, false, DataEncodingType.BASE64, true);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    // Read from checkpoint with collector file name which does not exist
    // but the collector file time stamp is within the stream
    th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
          CollectorStreamReader.getCollectorFile(doesNotExist2), 40), cluster,
          buffer, testStream, null, 10, 1000,
          false, DataEncodingType.BASE64, true);
      preader.init();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    //Read from startTime in local stream directory, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from startTime in local stream directory, with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from startTime in collector dir, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);    

    //Read from startTime in collector dir, with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);

    //Read from startTime in collector dir, with no timestamp file,
    // with no checkpoint
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);    

    //Read from startTime in collector dir, with no timestamp file,
    //with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);

    //Read from startTime beyond the stream
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from startTime after the stream
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3),
        1000, 1000, false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());

    //Read from startTime beyond the stream, with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1),
        1000, 1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from startTime after the stream, with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3),
        1000, 1000, false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());

  }

  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 1000, false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[0]), 1,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer,
        testStream, null, 1000, 1000, false,
        DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        20, 80, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 20), cluster,
        buffer, testStream, null, 1000, 1000,
        false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[0]), 1,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist2() throws Exception {
    Throwable th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
          CollectorStreamReader.getCollectorFile(doesNotExist2), 20), cluster,
          buffer, testStream, null, 1000, 1000,
        false, DataEncodingType.BASE64, true);
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
          CollectorStreamReader.getCollectorFile(doesNotExist3), 20), cluster,
          buffer, testStream, null, 1000, 1000,
        false, DataEncodingType.BASE64, true);
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
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
  }

  @Test
  public void testReadFromStartTimeWithinStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[0]), 1,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        1000, false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
  }

}
