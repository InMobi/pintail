package com.inmobi.databus.readers;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public abstract class TestAbstractDatabusWaitingReader {
  protected static final String testStream = "testclient";

  protected static final String collectorName = "collector1";
  protected static final String clusterName = "testCluster";
  protected PartitionId partitionId = new PartitionId(clusterName, null);
  protected DatabusStreamWaitingReader lreader;
  protected Cluster cluster;
  protected String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  protected Path[] databusFiles = new Path[3];
  protected String doesNotExist1 = TestUtil.files[0];
  protected String doesNotExist2 = TestUtil.files[2];
  protected String doesNotExist3 = TestUtil.files[7];
  FileSystem fs;

  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  abstract Path getStreamsDir();

  public void testInitialize() throws Exception {
    // Read from start
    lreader = new DatabusStreamWaitingReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        getStreamsDir(), 1000, false);
    lreader.build(CollectorStreamReader.getDateFromCollectorFile(files[0]));

    lreader.initFromStart();
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[0]);

    // Read from checkpoint with local stream file name
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        databusFiles[1].getName(), 20));
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[1]);

    // Read from checkpoint with local stream file name, which does not exist
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        DatabusStreamWaitingReader.getDatabusStreamFileName(collectorName,
            doesNotExist1), 20));
    Assert.assertEquals(lreader.getCurrentFile(), databusFiles[0]);

    // Read from checkpoint with local stream file name, which does not exist
    // with file timestamp after the stream
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        DatabusStreamWaitingReader.getDatabusStreamFileName(collectorName,
            doesNotExist3), 20));
    Assert.assertNull(lreader.getCurrentFile());  

    // Read from checkpoint with local stream file name, which does not exist
    // with file timestamp within the stream
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        DatabusStreamWaitingReader.getDatabusStreamFileName(collectorName,
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

  static void readFile(StreamReader reader, int fileNum,
      int startIndex, Path filePath)
      throws Exception {
    int fileIndex = fileNum * 100 ;
    for (int i = startIndex; i < 100; i++) {
      String line = reader.readLine();
      Assert.assertNotNull(line);
      Assert.assertEquals(new String(Base64.decodeBase64(line)),
          MessageUtil.constructMessage(fileIndex + i));
    }
    Assert.assertEquals(reader.getCurrentFile(), filePath);
  }


  public void testReadFromStart() throws Exception {
    lreader = new DatabusStreamWaitingReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        getStreamsDir(), 1000, false);
    lreader.build(CollectorStreamReader.getDateFromCollectorFile(files[0]));
    lreader.initFromStart();
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(lreader, 0, 0, databusFiles[0]);
    readFile(lreader, 1, 0, databusFiles[1]);
    readFile(lreader, 2, 0, databusFiles[2]);
    lreader.close();
  }

  public void testReadFromCheckpoint() throws Exception {
    lreader = new DatabusStreamWaitingReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        getStreamsDir(), 1000, false);
    PartitionCheckpoint pcp = new PartitionCheckpoint(
        DatabusStreamWaitingReader.getDatabusStreamFileName(collectorName, files[1]), 20);
    lreader.build(DatabusStreamReader.getBuildTimestamp( testStream, 
        DatabusStreamWaitingReader.getDatabusStreamFileName(collectorName, files[1])));
    lreader.initializeCurrentFile(pcp);
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(lreader, 1, 20, databusFiles[1]);
    readFile(lreader, 2, 0, databusFiles[2]);
    lreader.close();
  }

  public void testReadFromTimeStamp() throws Exception {
    lreader = new DatabusStreamWaitingReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        getStreamsDir(), 1000, false);
    lreader.build(CollectorStreamReader.getDateFromCollectorFile(files[1]));
    lreader.initializeCurrentFile(CollectorStreamReader.getDateFromCollectorFile(files[1]));
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(lreader, 1, 0, databusFiles[1]);
    readFile(lreader, 2, 0, databusFiles[2]);
    lreader.close();
  }

}
