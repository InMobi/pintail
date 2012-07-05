package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderHadoopStream {
  protected static final String testStream = "testclient";
  protected static final String fsName = "testCluster";
  protected PartitionId partitionId = new PartitionId(fsName, null);
  protected LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  protected PartitionReader preader;
  private Cluster cluster;

  protected String doesNotExist1 = TestUtil.files[0];
  protected String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  protected Path[] databusFiles = new Path[3];

  protected String doesNotExist2 = TestUtil.files[2];
  protected String doesNotExist3 = TestUtil.files[10];
  protected final String collectorName = "collector1";
  FileSystem fs;
  Path streamDir;
  Configuration conf = new Configuration();
  String inputFormatClass;

  @BeforeTest
  public void setup() throws Exception {
    // setup fs
    fs = FileSystem.getLocal(conf);
    String collectorName = "hadoopcollector";
    streamDir = new Path("/tmp/test/hadoop/" + this.getClass().getSimpleName(),
         testStream).makeQualified(fs);
    cluster = TestUtil.setupHadoopCluster(
        testStream, collectorName, conf, files, databusFiles, streamDir);
    inputFormatClass = SequenceFileInputFormat.class.getName();
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    // Read from start
    preader = new PartitionReader(partitionId, null, fs, buffer,
        testStream, streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 1000,
        DataEncodingType.NONE);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[0]);

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(
            DatabusStreamWaitingReader.getDatabusStreamFileName(
            collectorName, files[1]), 20),
            fs, buffer, testStream, streamDir, conf, inputFormatClass, null,
            1000, DataEncodingType.NONE);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    // Read from checkpoint with local stream file name which does not exist
    // and is before the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        DatabusStreamWaitingReader.getDatabusStreamFileName(collectorName,
            doesNotExist1), 20),
            fs, buffer, testStream, streamDir, conf, inputFormatClass, null,
            1000, DataEncodingType.NONE);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[0]);

    //Read from startTime in local stream directory, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000,
        DataEncodingType.NONE);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    //Read from startTime in local stream directory, with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000,
        DataEncodingType.NONE);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    //Read from startTime in local stream directory, with no timestamp file,
    // with no checkpoint
    preader = new PartitionReader(partitionId, null, fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        DataEncodingType.NONE);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    //Read from startTime in local stream directory, with no timestamp file,
    //with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        DataEncodingType.NONE);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[1]);

    //Read from startTime beyond the stream
    preader = new PartitionReader(partitionId, null, fs, buffer, testStream,
         streamDir, conf, inputFormatClass,
         CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
         DataEncodingType.NONE);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[0]);

    //Read from startTime after the stream
    preader = new PartitionReader(partitionId, null, fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        DataEncodingType.NONE, true);
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
        new PartitionCheckpoint(files[0], 10), fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        DataEncodingType.NONE);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile(), databusFiles[0]);

    //Read from startTime after the stream, with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(files[0], 10), fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3),
        1000, DataEncodingType.NONE, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());
  }

  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, null, fs, buffer,
        testStream, streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, DataEncodingType.NONE, true);
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

  @Test
  public void testReadFromCheckpoint() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), fs, buffer, testStream, streamDir,
        conf, inputFormatClass, null, 1000, DataEncodingType.NONE, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 20, 80, partitionId, buffer);
    TestUtil.assertBuffer(databusFiles[2].getName(), 3, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        DatabusStreamWaitingReader.getDatabusStreamFileName(collectorName, doesNotExist1),
        20), fs, buffer, testStream, streamDir, conf, 
        inputFormatClass, null, 1000, DataEncodingType.NONE, true);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
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
        DatabusStreamWaitingReader.class.getName());
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist2() throws Exception {
    Throwable th = null;
    try {
      preader = new PartitionReader(partitionId, new PartitionCheckpoint(
          doesNotExist2, 20), fs, buffer, testStream, streamDir, conf,
          inputFormatClass, null, 1000, DataEncodingType.NONE, true);
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
          doesNotExist3, 20), fs, buffer, testStream, streamDir, conf, 
          inputFormatClass, null, 1000, DataEncodingType.NONE, true);
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
        databusFiles[1].getName(), 20), fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000,
        DataEncodingType.NONE, true);
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

  @Test
  public void testReadFromStartTimeWithinStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        DataEncodingType.NONE, true);
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

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        DataEncodingType.NONE, true);
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

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        databusFiles[1].getName(), 20), fs, buffer, testStream,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        DataEncodingType.NONE, true);
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
