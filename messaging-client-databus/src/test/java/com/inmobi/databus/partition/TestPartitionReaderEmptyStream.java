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
import com.inmobi.databus.readers.DatabusStreamReader;
import com.inmobi.databus.readers.LocalStreamReader;
import com.inmobi.databus.readers.MergedStreamReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderEmptyStream {

  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);
  private PartitionId clusterId = new PartitionId(clusterName, null);

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
    testInitialize(partitionId, true, CollectorReader.class.getName(),
        CollectorStreamReader.class.getName());

    testInitialize(clusterId, true, ClusterReader.class.getName(),
        LocalStreamReader.class.getName());
    
    testInitialize(clusterId, false, ClusterReader.class.getName(),
        MergedStreamReader.class.getName());
  }
  
  public void testInitialize(PartitionId partitionId, boolean isLocal,
      String psreader, String sreader) throws Exception {
    // Read from start time 
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream,
        CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[0]),
        1000, isLocal, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        psreader);
    Assert.assertEquals(((AbstractPartitionStreamReader)preader
        .getReader()).getReader().getClass().getName(), sreader);

    //Read from checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        DatabusStreamReader.getDatabusStreamFileName(collectorName,
            TestUtil.files[1]), 20),
        cluster, buffer, testStream, null, 1000, isLocal, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        psreader);
    Assert.assertEquals(((AbstractPartitionStreamReader)preader
        .getReader()).getReader().getClass().getName(), sreader);

    //Read from startTime with checkpoint
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        DatabusStreamReader.getDatabusStreamFileName(collectorName, 
            TestUtil.files[0]), 20),
        cluster, buffer, testStream,
        CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[1]), 1000,
        isLocal, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        psreader);
    Assert.assertEquals(((AbstractPartitionStreamReader)preader
        .getReader()).getReader().getClass().getName(), sreader);
  }
}
