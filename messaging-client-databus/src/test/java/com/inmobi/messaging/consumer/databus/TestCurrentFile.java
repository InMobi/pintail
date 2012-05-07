package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;

public class TestCurrentFile {
  private static final String testStream = "testclient";
  private BlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);

  private String collectorName = "collector1";
  private String clusterName;
  private FileSystem fs;
  private Cluster cluster;
  private Path collectorDir;
  private int msgIndex = 0;
  private PartitionReader preader;
  private PartitionId partitionId;
  private String doesNotExist1 = testStream + "-2012-05-02-14-24_00000";
  private String file1 = testStream + "-2012-05-02-14-26_00000";
  private String file2 = testStream + "-2012-05-02-14-27_00000";
  private String file3 = testStream + "-2012-05-02-14-28_00000";
  private String currentScribeFile = testStream + "-2012-05-02-14-29_00000";
  private String doesNotExist2 = testStream + "-2012-05-02-14-30_00000";
  MiniDFSCluster dfsCluster;
  Configuration conf = new Configuration();
  
  private void createMessageFile(String fileName) throws IOException {
    FSDataOutputStream out = fs.create(new Path(collectorDir, fileName));
    for (int i = 0; i < 100; i++) {
      out.write(Base64.encodeBase64(constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }
    out.close();
  }

  
  private void writeMessages(FSDataOutputStream out, int num)
      throws IOException {
    for (int i = 0; i < num; i++) {
      out.write(Base64.encodeBase64(constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }  
    out.sync();
  }

  private void writeCurrentScribeFileName() throws IOException {
    FSDataOutputStream scribe = fs.create(
        new Path(collectorDir, testStream + "_current"));
    scribe.write(currentScribeFile.getBytes());
    scribe.write('\n');
    scribe.close();
  }
  
  private String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    str.append(index).append("Message");
    return str.toString();
  }

  @AfterTest
  public void cleanup() throws IOException {
    if (fs != null) {
      fs.delete(cluster.getDataDir(), true);
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }

  }
  
  private void assertBuffer(String fileName, int fileNum, int startIndex,
      int numMessages)
      throws InterruptedException {
    int fileIndex = (fileNum - 1) * 100 ;
    for (int i = startIndex; i < startIndex + numMessages; i++) {
      QueueEntry entry = buffer.take();
      Assert.assertEquals(entry.partitionId, partitionId);
      Assert.assertEquals(entry.partitionChkpoint,
          new PartitionCheckpoint(fileName, i + 1));
      Assert.assertEquals(new String(entry.message.getData().array()),
        constructMessage(fileIndex + i));
    }
  }
  

  @BeforeTest
  public void setup() throws Exception {     
    dfsCluster = new MiniDFSCluster(conf, 1, true,null);
      // initialize config
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add(testStream);
    cluster = new Cluster("testDFSCluster", "/tmp/databus",
      dfsCluster.getFileSystem().getUri().toString(), "local", null,
      sourceNames);
    Path streamDir = new Path(cluster.getDataDir(), testStream);
    partitionId = new PartitionId(clusterName, collectorName);
      
    // setup stream and collector dirs
    fs = FileSystem.get(cluster.getHadoopConf());
    collectorDir = new Path(streamDir, collectorName);
    fs.mkdirs(collectorDir);

    // setup data dirs
    createMessageFile(file1);
    createMessageFile(file2);
    createMessageFile(file3);
  }

  @Test
  public void testReadFromCurrentScribeFile() throws Exception {
    writeCurrentScribeFileName();
    FSDataOutputStream out = fs.create(
        new Path(collectorDir, currentScribeFile));
    writeMessages(out, 10);
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        null, -1), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamFileReader.class.getName());
    preader.start();
    assertBuffer(file1, 1, 0, 100);
    assertBuffer(file2, 2, 0, 100);
    assertBuffer(file3, 3, 0, 100);
    assertBuffer(currentScribeFile, 4, 0, 10);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
            CollectorStreamFileReader.class.getName());
    writeMessages(out, 20);
    assertBuffer(currentScribeFile, 4, 10, 20);
    writeMessages(out, 20);
    assertBuffer(currentScribeFile, 4, 30, 20);
    writeMessages(out, 50);
    out.close();
    assertBuffer(currentScribeFile, 4, 50, 50);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getCurrentReader());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
            CollectorStreamFileReader.class.getName());
    preader.close();
  }

}
