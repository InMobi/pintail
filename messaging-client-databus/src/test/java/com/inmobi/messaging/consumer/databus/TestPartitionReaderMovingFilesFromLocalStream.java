package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;

public class TestPartitionReaderMovingFilesFromLocalStream {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(150);
  private Cluster cluster;
  private PartitionReader preader;
  private FileSystem fs;

  private String[] files = new String[] {TestUtil.files[1],
      TestUtil.files[2], TestUtil.files[3], TestUtil.files[4],
      TestUtil.files[5], TestUtil.files[6]};

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, 4);
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
        1000, 10);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());

    preader.start();
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    fs.delete(TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        files[0]), true);
    fs.delete(TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        files[1]), true);
    fs.delete(TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        files[2]), true);

    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[0]), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[1]), 2, 0, 50, partitionId, buffer);

    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[1]), 2, 50, 50, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, files[3]), 4, 0, 100, partitionId, buffer);

    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    TestUtil.assertBuffer(files[4], 5, 0, 100, partitionId, buffer);    
    TestUtil.assertBuffer(files[5], 6, 0, 50, partitionId, buffer);
    TestUtil.assertBuffer(files[5], 6, 50, 50, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
  }
}
