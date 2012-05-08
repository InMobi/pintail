package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
  private Path streamDir;
  private PartitionReader preader;
  private FileSystem fs;

  private String file1 =  testStream + "-2012-05-02-14-26_00000";
  private String file2 =  testStream + "-2012-05-02-14-27_00000";
  private String file3 =  testStream + "-2012-05-02-14-28_00000";
  private String file4 =  testStream + "-2012-05-02-14-29_00000";
  private String file41 = testStream + "-2012-05-02-14-30_00000";
  private String file5 =  testStream + "-2012-05-02-14-31_00000";
  private String file51 = testStream + "-2012-05-02-14-32_00000";
  private String file6 =  testStream + "-2012-05-02-14-33_00000";
  private String file61 = testStream + "-2012-05-02-14-34_00000";

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId,
        new String[] {file1, file2, file3, file4, file5, file6},
        new String[] {file41, file51, file61}, 4);
    streamDir = new Path(new Path(cluster.getLocalFinalDestDirRoot(), testStream),
        collectorName);
    fs = FileSystem.get(cluster.getHadoopConf());
  }
  
  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }
  
  @Test
  public void testLocalStreamFileMoved() throws Exception {
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(null, -1), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());

    preader.execute();
    fs.delete(TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        file1), true);
    fs.delete(TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        file2), true);
    fs.delete(TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
        file3), true);

    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file1), 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file2), 2, 0, 50, partitionId, buffer);
    preader.execute();
    
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();

    TestUtil.assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file4), 4, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(file5, 5, 0, 50, partitionId, buffer);    

    preader.execute();
    TestUtil.assertBuffer(file5, 5, 50, 50, partitionId, buffer);    
    TestUtil.assertBuffer(file6, 6, 0, 100, partitionId, buffer);
    preader.execute();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }
}
