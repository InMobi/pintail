package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderMovingFilesFromLocalStream {
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

  private String[] files = new String[] {TestUtil.files[1],
      TestUtil.files[2], TestUtil.files[3], TestUtil.files[4],
      TestUtil.files[5], TestUtil.files[6], TestUtil.files[7],
      TestUtil.files[8]};
  private Path[] databusFiles = new Path[8];

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, databusFiles, 4);
    collectorDir = new Path(new Path(cluster.getDataDir(), testStream),
        collectorName);
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testLocalStreamFileMoved() throws Exception {
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 1000, false);
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
    fs.delete(databusFiles[0], true);
    fs.delete(databusFiles[1], true);
    fs.delete(databusFiles[2], true);
    databusFiles[4] = TestUtil.moveFileToStreamLocal(fs, testStream,
        collectorName, cluster, collectorDir, files[4]);
    databusFiles[5] = TestUtil.moveFileToStreamLocal(fs, testStream,
        collectorName, cluster, collectorDir, files[5]);

    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, files[1]), 2, 0, 50, partitionId, buffer);

    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, files[1]), 2, 50, 50, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, files[3]), 4, 0, 100, partitionId, buffer);

    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    fs.delete(databusFiles[3], true);
    fs.delete(databusFiles[4], true);
    fs.delete(databusFiles[5], true);
    databusFiles[6] = TestUtil.copyFileToStreamLocal(fs, testStream,
        collectorName, cluster, collectorDir, files[6]);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, files[4]), 5, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, files[5]), 6, 0, 50, partitionId, buffer);
    Assert.assertEquals(((CollectorReader)preader.getReader()).
        getReader().getClass().getName(),
        LocalStreamCollectorReader.class.getName());
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    fs.delete(databusFiles[6], true);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, files[5]), 6, 50, 50, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, files[6]), 7, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(files[7], 8, 0, 100, partitionId, buffer);
    Assert.assertEquals(((CollectorReader)preader.getReader())
        .getReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    Assert.assertTrue(buffer.isEmpty());
    preader.close();
  }
}
