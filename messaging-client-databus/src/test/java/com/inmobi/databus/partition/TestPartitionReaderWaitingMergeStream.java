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
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.MergedStreamReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderWaitingMergeStream {

  private static final String testStream = "testclient";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, null);

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(150);
  private Cluster cluster;
  private PartitionReader preader;
  private boolean isLocal = false;

  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  private Path[] databusFiles = new Path[3];

  private final String collectorName = "collector1";
  FileSystem fs;

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        databusFiles, 0, 3);
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, isLocal, false);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        MergedStreamReader.class.getName());
    preader.start();
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    fs.delete(databusFiles[0], true);
    fs.delete(databusFiles[1], true);
    fs.delete(databusFiles[2], true);
    Path[] newDatabusFiles = new Path[3];
    TestUtil.setUpFiles(cluster, collectorName, new String[] {TestUtil.files[6]
        }, null, newDatabusFiles, 0, 1);
    TestUtil.assertBuffer(databusFiles[0].getName(), 1, 0, 100, partitionId,
        buffer);
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 0, 50, partitionId,
        buffer);
    
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    TestUtil.assertBuffer(databusFiles[1].getName(), 2, 50, 50, partitionId,
        buffer);
    TestUtil.assertBuffer(newDatabusFiles[0].getName(), 1, 0, 100, partitionId,
        buffer);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        MergedStreamReader.class.getName());
    TestUtil.setUpFiles(cluster, collectorName, new String[] {TestUtil.files[7],
        TestUtil.files[8]}, null, newDatabusFiles, 0, 2);
    TestUtil.assertBuffer(newDatabusFiles[0].getName(), 1, 0, 100, partitionId,
        buffer);
    TestUtil.assertBuffer(newDatabusFiles[1].getName(), 2, 0, 100, partitionId,
        buffer);
    Assert.assertTrue(buffer.isEmpty());    
    preader.close();
  }
}
