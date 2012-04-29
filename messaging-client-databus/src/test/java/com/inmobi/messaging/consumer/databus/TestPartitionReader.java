package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.SourceStream;

public class TestPartitionReader {
  private static final String testStream = "testclient";
  private BlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);

  private String collectorName = "collector1";
  private String clusterName;
  private DatabusConfig databusConfig;
  private FileSystem fs;
  private Cluster cluster;
  private Path collectorDir;
  private int msgIndex = 0;
  private PartitionReader preader;
  private PartitionId partitionId;
  
  @BeforeSuite
  public void setup() throws Exception {
    // initialize config
      DatabusConfigParser parser = new DatabusConfigParser(null);
      databusConfig = parser.getConfig();
      SourceStream sourceStream = 
          databusConfig.getSourceStreams().get(testStream);
      clusterName = sourceStream.getSourceClusters().iterator().next();
      cluster = databusConfig.getClusters().get(clusterName);
      Path streamDir = new Path(cluster.getDataDir(), testStream);
      partitionId = new PartitionId(clusterName, collectorName);
      
      // setup stream and collector dirs
      fs = FileSystem.get(cluster.getHadoopConf());
      collectorDir = new Path(streamDir, collectorName);
      fs.mkdirs(collectorDir);

      // setup data dirs
      createMessageFile("1");
      createMessageFile("2");
      createMessageFile("3");
  }
  
  @AfterSuite
  public void cleanup() throws IOException {
    fs.delete(cluster.getDataDir(), true);
  }
  private void createMessageFile(String fileNum) throws IOException {
    FSDataOutputStream out = fs.create(new Path(collectorDir, fileNum));
    for (int i = 0; i < 100; i++) {
      out.write(Base64.encodeBase64(constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }
    out.close();
  }
  
  private String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    str.append(index).append("Message");
    return str.toString();
  }
  
  private void assertBuffer(String fileNum, int startIndex)
      throws InterruptedException {
    int fileIndex = (Integer.parseInt(fileNum) - 1) * 100 ;
    for (int i = startIndex; i < 100; i++) {
      QueueEntry entry = buffer.take();
      Assert.assertEquals(entry.partitionId, partitionId);
      Assert.assertEquals(entry.partitionChkpoint,
          new PartitionCheckpoint(fileNum, i + 1));
      Assert.assertEquals(new String(entry.message.getData().array()),
        constructMessage(fileIndex + i));
    }
  }
  
  public void testInitialize() throws Exception {
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(null, -1), databusConfig, buffer, testStream);
    Assert.assertNull(preader.getCurrentFile());
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir, "1"));
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint("2", 20), databusConfig, buffer, testStream);
    preader.initializeCurrentFile();
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir, "2"));
  }
  
  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(null, -1), databusConfig, buffer, testStream);
    preader.execute();
    assertBuffer("1", 0);
    preader.execute();
    assertBuffer("2", 0);
    preader.execute();
    assertBuffer("3", 0);
  }
  
  @Test
  public void testReadFromCheckpoint() throws Exception {
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint("2", 20), databusConfig, buffer, testStream);
    preader.execute();
    assertBuffer("2", 20);
    preader.execute();
    assertBuffer("3", 0);
  }

}
