package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public abstract class TestAbstractWaitingClusterReader {

  protected static final String testStream = "testclient";
  protected static final String clusterName = "testCluster";
  protected PartitionId partitionId = new PartitionId(clusterName, null);

  protected LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(150);
  protected Cluster cluster;
  protected PartitionReader preader;

  protected String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  protected Path[] databusFiles = new Path[3];

  protected final String collectorName = "collector1";
  FileSystem fs;

  abstract void setupFiles(String[] files, Path[] newDatabusFiles) throws Exception;
  abstract boolean isLocal();
  
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        1000, 1000, isLocal(), DataEncodingType.BASE64, false);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.start();
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    FileStatus fs0 = fs.getFileStatus(databusFiles[0]); 
    FileStatus fs1 = fs.getFileStatus(databusFiles[1]); 
    fs.delete(databusFiles[0], true);
    fs.delete(databusFiles[1], true);
    fs.delete(databusFiles[2], true);
    Path[] newDatabusFiles = new Path[3];
    setupFiles(new String[] {TestUtil.files[6]}, newDatabusFiles);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs0), 1, 0, 100, partitionId,
        buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs1), 2, 0, 50, partitionId,
        buffer, true);
    
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs1), 2, 50, 50, partitionId,
        buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(newDatabusFiles[0])), 1, 0, 100, partitionId,
        buffer, true);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(((ClusterReader)preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    setupFiles(new String[] {TestUtil.files[7], TestUtil.files[8]},
        newDatabusFiles);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(newDatabusFiles[0])), 1, 0, 100, partitionId,
        buffer, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(newDatabusFiles[1])), 2, 0, 100, partitionId,
        buffer, true);
    Assert.assertTrue(buffer.isEmpty());    
    preader.close();
  }
}
