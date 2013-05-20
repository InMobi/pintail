package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class TestPartitionReaderMovingFilesWithStopTime {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(149);
  private Cluster cluster;
  private PartitionReader preader;
  private FileSystem fs;
  private Path collectorDir;
  private Path streamsLocalDir;
  private Configuration conf = new Configuration();

  private String[] files = new String[] {TestUtil.files[1],
      TestUtil.files[2], TestUtil.files[3], TestUtil.files[4],
      TestUtil.files[5], TestUtil.files[6], TestUtil.files[7],
      TestUtil.files[8]};
  private Path[] databusFiles = new Path[8];
  int consumerNumber;

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    consumerNumber = 1;
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, databusFiles, 4);
    collectorDir = DatabusUtil.getCollectorStreamDir(
        new Path(cluster.getRootDir()), testStream,
        collectorName);
    streamsLocalDir = DatabusUtil.getStreamDir(StreamType.LOCAL,
        new Path(cluster.getRootDir()), testStream);
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testLocalStreamFileMovedWithStopDate() throws Exception {
    CollectorReaderStatsExposer prMetrics = new CollectorReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber);
    preader = new PartitionReader(partitionId, null, conf, fs,
        collectorDir, streamsLocalDir, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 1000, prMetrics, CollectorStreamReader.getDateFromCollectorFile(files[6]));

    Assert.assertEquals(preader.getReader().getClass().getName(),
        CollectorReader.class.getName());
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());

    preader.start();
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    // delete a file from streams local while reader is reading
    fs.delete(databusFiles[2], true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2, 0, 50, partitionId, buffer, true);
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[1]), 2, 50, 50, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[3]), 4, 0, 100, partitionId, buffer, true);
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    // move files to local stream while reader is reading these files
    databusFiles[4] = TestUtil.moveFileToStreamLocal(fs, testStream,
        collectorName, cluster, collectorDir, files[4]);
    databusFiles[5] = TestUtil.moveFileToStreamLocal(fs, testStream,
        collectorName, cluster, collectorDir, files[5]);
    databusFiles[6] = TestUtil.moveFileToStreamLocal(fs, testStream,
        collectorName, cluster, collectorDir, files[6]);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[4]),
        5, 0, 100, partitionId, buffer, true);
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]),
        6, 0, 50, partitionId, buffer, true);
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    TestUtil.assertBuffer(CollectorStreamReader.getCollectorFile(files[5]),
        6, 50, 50, partitionId, buffer, true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFile(
        collectorName, files[6]), 7, 0, 100, partitionId, buffer, true);
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 600);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 600);
    Assert.assertEquals(prMetrics.getSwitchesFromLocalToCollector(), 1);
    Assert.assertEquals(prMetrics.getSwitchesFromCollectorToLocal(), 1);
  }
}
