package com.inmobi.databus.readers;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestLocalStreamReader {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, null);
  private LocalStreamReader lreader;
  private Cluster cluster;
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
                                         TestUtil.files[5]};
  private Path[] databusFiles = new Path[3];
  private String doesNotExist1 = TestUtil.files[0];
  private String doesNotExist2 = TestUtil.files[2];
  private String doesNotExist3 = TestUtil.files[7];
  private FileSystem fs;

  @BeforeTest
  public void setup() throws Exception {
    // initialize config
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
    testStream, new PartitionId(clusterName, collectorName), files, null,
    databusFiles, 3, 0);
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    // Read from start
    lreader = new LocalStreamReader(partitionId, cluster, testStream, 1000, false);
    lreader.build(CollectorStreamReader.getDateFromCollectorFile(files[0]));

    lreader.initFromStart();
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[0]);

    // Read from checkpoint with local stream file name
    lreader.initializeCurrentFile(new PartitionCheckpoint(
    databusFiles[1].getName(), 20));
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[1]);

    // Read from checkpoint with local stream file name, which does not exist
    lreader.initializeCurrentFile(new PartitionCheckpoint(
    LocalStreamReader.getDatabusStreamFileName(collectorName,
    doesNotExist1), 20));
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[0]);

    // Read from checkpoint with local stream file name, which does not exist
    // with file timestamp after the stream
    lreader.initializeCurrentFile(new PartitionCheckpoint(
    LocalStreamReader.getDatabusStreamFileName(collectorName,
    doesNotExist3), 20));
    Assert.assertNull(lreader.getCurrentFile());

    // Read from checkpoint with local stream file name, which does not exist
    // with file timestamp within the stream
    lreader.initializeCurrentFile(new PartitionCheckpoint(
    LocalStreamReader.getDatabusStreamFileName(collectorName,
    doesNotExist2), 20));
    Assert.assertNull(lreader.getCurrentFile());

    //Read from startTime in local stream directory 
    lreader.initializeCurrentFile(
    CollectorStreamReader.getDateFromCollectorFile(files[1]));
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[1]);

    //Read from startTime in local stream directory, before the stream
    lreader.initializeCurrentFile(
    CollectorStreamReader.getDateFromCollectorFile(doesNotExist1));
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[0]);

    //Read from startTime in local stream directory, within the stream
    lreader.initializeCurrentFile(
    CollectorStreamReader.getDateFromCollectorFile(doesNotExist2));
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[1]);

    //Read from startTime in after the stream
    lreader.initializeCurrentFile(
    CollectorStreamReader.getDateFromCollectorFile(doesNotExist3));
    Assert.assertNull(lreader.getCurrentFile());

    // startFromNextHigher with filename
    lreader.startFromNextHigher(fs.getFileStatus(databusFiles[1]));
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[2]);

    // startFromNextHigher with date
    lreader.startFromTimestmp(
    CollectorStreamReader.getDateFromCollectorFile(files[1]));
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[1]);

    // startFromBegining 
    lreader.startFromBegining();
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[0]);
  }

  private void readFile(int fileNum, int startIndex) throws Exception {
    int fileIndex = fileNum * 100 ;
    for (int i = startIndex; i < 100; i++) {
      String line = lreader.readLine();
      Assert.assertNotNull(line);
      Assert.assertEquals(new String(Base64.decodeBase64(line)),
      MessageUtil.constructMessage(fileIndex + i));
    }
    Assert.assertEquals(lreader.getCurrentFile(),
    databusFiles[fileNum]);
  }

  @Test
  public void testReadFromStart() throws Exception {
    lreader = new LocalStreamReader(partitionId, cluster, testStream, 1000, false);
    lreader.build(CollectorStreamReader.getDateFromCollectorFile(files[0]));
    lreader.initFromStart();
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(0, 0);
    readFile(1, 0);
    readFile(2, 0);
    lreader.close();
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    lreader = new LocalStreamReader(partitionId, cluster, testStream, 1000, false);
    PartitionCheckpoint pcp = new PartitionCheckpoint(
    LocalStreamReader.getDatabusStreamFileName(collectorName, files[1]), 20);
    lreader.build(DatabusStreamReader.getBuildTimestamp( testStream,
    LocalStreamReader.getDatabusStreamFileName(collectorName, files[1])));
    lreader.initializeCurrentFile(pcp);
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(1, 20);
    readFile(2, 0);
    lreader.close();
  }

  @Test
  public void testReadFromTimeStamp() throws Exception {
    lreader = new LocalStreamReader(partitionId, cluster,  testStream, 1000, false);
    lreader.build(CollectorStreamReader.getDateFromCollectorFile(files[1]));
    lreader.initializeCurrentFile(CollectorStreamReader.getDateFromCollectorFile(files[1]));
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(1, 0);
    readFile(2, 0);
    lreader.close();
  }

}
