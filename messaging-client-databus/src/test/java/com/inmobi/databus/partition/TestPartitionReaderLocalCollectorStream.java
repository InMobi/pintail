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
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderLocalCollectorStream {
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
  private Path[] databusFiles = new Path[3];
  private String doesNotExist1 = TestUtil.files[0];
  private String doesNotExist2 = TestUtil.files[3];
  private String doesNotExist3 = TestUtil.files[7];

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, databusFiles, 3);
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
        1000, 1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    // Read from checkpoint with collector file name, but file in local stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer,
        testStream, null, 1000, 1000, false,
        DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(LocalStreamCollectorReader.getDatabusStreamFile(
            collectorName, files[1]), 20),
            cluster, buffer, testStream, null, 1000, 1000, false,
            DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    // Read from checkpoint with collector file name which does not exist
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), cluster,
        buffer, testStream, null, 1000, 1000,
        false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    // Read from checkpoint with local stream file name which does not exist
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist1), 20),
            cluster, buffer, testStream, null, 1000, 1000, false,
            DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

    //Read from startTime in local stream directory, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    //Read from startTime in local stream directory, with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[1].getName());

    //Read from startTime in local stream directory, with no timestamp file,
    // with no checkpoint
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[2].getName());

    //Read from startTime in local stream directory, with no timestamp file,
    //with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[2].getName());

    //Read from startTime beyond the stream
    preader = new PartitionReader(partitionId,
        null, cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

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
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, false, DataEncodingType.BASE64);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        databusFiles[0].getName());

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
        10, 1000, false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }

  @Test
  public void testReadFromCheckpointWithCollectorFileName() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer, 
        testStream, null, 10, 1000, false,
        DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  20, 80, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }

  @Test
  public void testReadFromCheckpointWithLocalStreamFileName() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            files[1]), 20),
        cluster, buffer, testStream, null, 10, 1000, false,
        DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  20, 80, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
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
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), cluster,
        buffer, testStream, null, 10, 1000, false,
        DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
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
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist1), 20),
            cluster, buffer, testStream, null, 10, 1000, false,
            DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }


  @Test
  public void testReadFromStartTimeInLocalStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 20), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 10, 1000,
        false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeInLocalStream2() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 20), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 10,
        1000, false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1),
        10, 1000, false, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2,  0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[2]), 3,  0, 100, partitionId, buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.close();
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3),
        10, 1000, false, DataEncodingType.BASE64, true);
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
