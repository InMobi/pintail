package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;

public class TestPartitionReaderEmptyStream {

  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  private Cluster cluster;
  private PartitionReader preader;

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, null, null, 0);
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    // Read from start time
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[0]),
        1000, true);
    preader.initializeCurrentFile();
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());

    //Read from checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        TestUtil.files[1], 20), cluster, buffer, testStream, null, 1000, true);
    preader.initializeCurrentFile();
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());

    //Read from startTime with checkpoint
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(TestUtil.files[0], 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[1]), 1000,
        true);
    preader.initializeCurrentFile();
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
  }
}
