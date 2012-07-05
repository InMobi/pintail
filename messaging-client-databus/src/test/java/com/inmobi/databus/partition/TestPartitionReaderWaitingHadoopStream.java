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
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderWaitingHadoopStream {

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
  Path streamDir;
  Configuration conf = new Configuration();
  String inputFormatClass;

  @BeforeTest
  public void setup() throws Exception {
    // setup fs
    fs = FileSystem.getLocal(conf);
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
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, null, fs, buffer,
        testStream, streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(files[0]), 1000,
        DataEncodingType.NONE);
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
    fs.delete(databusFiles[0], true);
    fs.delete(databusFiles[1], true);
    fs.delete(databusFiles[2], true);
    Path[] newDatabusFiles = new Path[3];
    TestUtil.setUpHadoopFiles(streamDir, testStream, collectorName, cluster,
        conf,  new String[] {TestUtil.files[6]}, newDatabusFiles);
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
        DatabusStreamWaitingReader.class.getName());
    TestUtil.setUpHadoopFiles(streamDir, testStream, collectorName, cluster,
        conf,  new String[] {TestUtil.files[7], TestUtil.files[8]},
        newDatabusFiles);
    TestUtil.assertBuffer(newDatabusFiles[0].getName(), 1, 0, 100, partitionId,
        buffer);
    TestUtil.assertBuffer(newDatabusFiles[1].getName(), 2, 0, 100, partitionId,
        buffer);
    Assert.assertTrue(buffer.isEmpty());    
    preader.close();
  }
}
