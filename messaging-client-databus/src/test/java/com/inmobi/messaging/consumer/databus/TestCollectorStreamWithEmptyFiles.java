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

public class TestCollectorStreamWithEmptyFiles {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);
  private Path collectorDir;
  private Cluster cluster;
  private String[] files = new String[] {TestUtil.files[0]};
  private String[] emptyfiles = new String[] {TestUtil.files[1]};
  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(101);

  FileSystem fs;


  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, emptyfiles, 0, false);
    collectorDir = new Path(new Path(cluster.getDataDir(), testStream),
        collectorName);
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testReadFromStart() throws Exception {
    TestUtil.writeCurrentScribeFileName(fs, collectorDir, testStream,
        emptyfiles[0]);
    PartitionReader preader = new PartitionReader(partitionId, null, cluster, buffer,
        testStream, CollectorStreamReader.getDateFromCollectorFile(files[0]),
        5);
    preader.initializeCurrentFile();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.start();
    
    // Test reading from next file
    while (buffer.remainingCapacity() != 1) {
      Thread.sleep(10);
    }

    // test waiting for data in current scribe file by sleep for some more time
    Thread.sleep(20);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    String dataFile = TestUtil.files[2];
    
    // change the currentScribeFileName and create the file, sothat it sees the 
    // file after building file list 
    TestUtil.writeCurrentScribeFileName(fs, collectorDir, testStream,
        dataFile);
    TestUtil.setUpCollectorDataFiles(fs, collectorDir, dataFile);
    TestUtil.assertBuffer(files[0], 1, 0, 100, partitionId, buffer);
    TestUtil.assertBuffer(dataFile, 1, 0, 100, partitionId, buffer);
    
    // Test the path for current file getting created late.
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    String emptyFile = TestUtil.files[3];
    dataFile = TestUtil.files[4];
    TestUtil.writeCurrentScribeFileName(fs, collectorDir, testStream,
        emptyFile);
    TestUtil.setUpEmptyFiles(fs, collectorDir, emptyFile);
    Thread.sleep(20);
    TestUtil.writeCurrentScribeFileName(fs, collectorDir, testStream,
        dataFile);
    fs.delete(new Path(collectorDir, emptyfiles[0]), true);
    fs.delete(new Path(collectorDir, emptyFile), true);
    Thread.sleep(50);
    TestUtil.setUpCollectorDataFiles(fs, collectorDir, dataFile);
    TestUtil.assertBuffer(dataFile, 1, 0, 100, partitionId, buffer);
    
    //Test the path for next higher entry
    emptyFile = TestUtil.files[5];
    dataFile = TestUtil.files[6];
    TestUtil.writeCurrentScribeFileName(fs, collectorDir, testStream,
        emptyFile);
    TestUtil.setUpEmptyFiles(fs, collectorDir, emptyFile);
    Thread.sleep(20);
    TestUtil.writeCurrentScribeFileName(fs, collectorDir, testStream,
        dataFile);
    fs.delete(new Path(collectorDir, emptyFile), true);
    TestUtil.setUpCollectorDataFiles(fs, collectorDir, dataFile);
    TestUtil.assertBuffer(dataFile, 1, 0, 100, partitionId, buffer);
    Assert.assertTrue(buffer.isEmpty());
    preader.close();
  }

}
