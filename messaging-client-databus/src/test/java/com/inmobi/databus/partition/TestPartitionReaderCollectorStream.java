package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.EOFMessage;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class TestPartitionReaderCollectorStream {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer =
      new LinkedBlockingQueue<QueueEntry>(1000);
  private ClusterUtil cluster;
  private PartitionReader preader;

  private String doesNotExist1 = TestUtil.files[0];
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};

  private String doesNotExist2 = TestUtil.files[2];
  private String doesNotExist3 = TestUtil.files[10];
  private Path collectorDir;
  private Path streamsLocalDir;
  private Configuration conf = new Configuration();
  private FileSystem fs;
  int consumerNUmber;
  String fsUri;

  @BeforeTest
  public void setup() throws Exception {
    consumerNUmber = 1;
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, 0, TestUtil.getConfiguredRootDir());
    collectorDir = DatabusUtil.getCollectorStreamDir(
        new Path(cluster.getRootDir()), testStream,
        collectorName);
    streamsLocalDir = DatabusUtil.getStreamDir(StreamType.LOCAL,
        new Path(cluster.getRootDir()), testStream);
    fs = FileSystem.get(cluster.getHadoopConf());
    fsUri = fs.getUri().toString();
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    // Read from start
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir,
        buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 1000, 1000,
        prMetrics, null);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream, null,
        1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);

    //Read from startTime
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);

    //Read from checkpoint with start time
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 20), conf, fs,
        collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(
            LocalStreamCollectorReader.getDatabusStreamFile(
            collectorName, doesNotExist1), 20),
            conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
            1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    // Read from checkpoint with collector file name and file in collector
    // stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream, null,
        1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);

    // Read from checkpoint with collector file name which does not exist
    // and is before the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream,
        null, 1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    // Read from checkpoint with collector file name which does not exist,
    //is before the stream and with start time
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 40), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]),
        1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    // Read from checkpoint with local stream file name which does not exist
    // and is before the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFile(collectorName,
            doesNotExist1), 20),
            conf, fs, collectorDir, streamsLocalDir, buffer, testStream, null,
            1000, 1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    // Read from checkpoint with collector file name which does not exist
    // but the collector file time stamp is after the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist3), 40), conf,
        fs, collectorDir, streamsLocalDir, buffer,
        testStream, null, 1000, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertNull(preader.getCurrentFile());

    // Read from checkpoint with collector file name which does not exist
    // but the collector file time stamp is within the streams
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist2), 40), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream,
        null, 10, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);

    //Read from startTime in local stream directory, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from checkpoint, with startTime in local stream directory
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from startTime in collector dir, with no checkpoint
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);

    //Read from checkpoint, with startTime in collector dir
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir,
        streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from startTime in collector dir, with no timestamp file,
    // with no checkpoint
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir,
        streamsLocalDir, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);

    //Read from checkpoint, with startTime in collector dir, with no timestamp
    //file
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir,
        streamsLocalDir, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from startTime beyond the stream
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from startTime after the stream
    preader = new PartitionReader(partitionId,
        null, conf, fs, collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        1000, prMetrics, true, null);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());

    //Read from startTime beyond the stream
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, prMetrics, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[0]);

    //Read from checkpoint and startTime after the stream
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[0]), 10), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        1000, prMetrics, true, null);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertNotNull(preader.getCurrentFile());

  }

  @Test
  public void testReadFromStart() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 1000, 1000,
        prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[0]), 1,
        0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20), conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        20, 80, partitionId, buffer, true, null);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 180);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 180);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 20), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]),
        1000, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[0]), 1,
        0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist2() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist2), 20), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream,
        null, 1000, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(), files[1]);
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist3() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist3), 20), conf,
        fs, collectorDir, streamsLocalDir, buffer, testStream,
        null, 1000, 1000, prMetrics, true, null);
    preader.init();
    Assert.assertNull(preader.getCurrentFile());
  }

  @Test
  public void testReadFromStartTime() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(files[1]), 1000, 1000,
        prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 200);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
    Assert.assertEquals(prMetrics.getReadPathTime(),
        getPrevTimeStamp(CollectorStreamReader.getDateFromCollectorFile(files[2])).getTime());

  }

  private Date getPrevTimeStamp(Date currentFileTimeStamp) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(currentFileTimeStamp);
    cal.add(Calendar.MINUTE, -1);
    return cal.getTime();
  }
  @Test
  public void testReadFromStartTimeWithinStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist2), 1000,
        1000, prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 200);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist1), 1000,
        1000, prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[0]), 1,
        0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[1]), 2,
        0, 100, partitionId, buffer, true, null);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[2]), 3,
        0, 100, partitionId, buffer, true, null);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNUmber, fsUri);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3), 1000,
        1000, prMetrics, true, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    Assert.assertEquals(((CollectorReader) preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 0);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 0);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 0);
    Assert.assertEquals(prMetrics.getCumulativeNanosForFetchMessage(), 0);
    Assert.assertEquals(prMetrics.getReadPathTime(), 0);
  }
}
