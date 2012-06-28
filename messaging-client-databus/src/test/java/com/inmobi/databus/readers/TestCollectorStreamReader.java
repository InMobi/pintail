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

public class TestCollectorStreamReader {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);
  private Path collectorDir;
  private CollectorStreamReader cReader;
  private Cluster cluster;
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  private String doesNotExist1 = TestUtil.files[0];
  private String doesNotExist2 = TestUtil.files[2];
  private String doesNotExist3 = TestUtil.files[7];

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, 0);
    collectorDir = new Path(new Path(cluster.getDataDir(), testStream),
        collectorName);
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    TestUtil.createEmptyFile(fs, collectorDir, testStream + "_current");
    TestUtil.createEmptyFile(fs, collectorDir, "scribe_stats");
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    // Read from start
    cReader = new CollectorStreamReader(partitionId, cluster, testStream, 10,
        10, true);
    cReader.build();
    cReader.initFromStart();
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[0]));

    // Read from checkpoint with collector file name
    cReader.initializeCurrentFile(
        new PartitionCheckpoint(files[1], 20));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[1]));

    // Read from checkpoint with local stream file name
    cReader.initializeCurrentFile(new PartitionCheckpoint(
        LocalStreamCollectorReader.getDatabusStreamFileName(collectorName,
            files[1]), 20));
    Assert.assertNull(cReader.getCurrentFile());

    // Read from checkpoint with collector file name which does not exist
    // and is before the stream
    cReader.initializeCurrentFile(
        new PartitionCheckpoint(doesNotExist1, 20));
    Assert.assertNull(cReader.getCurrentFile());

    // Read from checkpoint with collector file name which does not exist
    // and is within the stream
    cReader.initializeCurrentFile(
        new PartitionCheckpoint(doesNotExist2, 20));
    Assert.assertNull(cReader.getCurrentFile());

    // Read from checkpoint with collector file name which does not exist
    // is after the stream
    cReader.initializeCurrentFile(
        new PartitionCheckpoint(doesNotExist3, 20));
    Assert.assertNull(cReader.getCurrentFile());

    //Read from startTime in collector dir
    cReader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(files[1]));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[1]));

    //Read from startTime in before the stream
    cReader.initializeCurrentFile(CollectorStreamReader.getDateFromCollectorFile(
        doesNotExist1));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[0]));

    //Read from startTime in within the stream
    cReader.initializeCurrentFile(CollectorStreamReader.getDateFromCollectorFile(
        doesNotExist2));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[1]));

    // Read from startTime after the stream
    cReader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(doesNotExist3));
    Assert.assertNull(cReader.getCurrentFile());
    
    // startFromNextHigher with filename
    cReader.startFromNextHigher(files[1]);
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[2]));

    // startFromNextHigher with date
    cReader.startFromTimestmp(
        CollectorStreamReader.getDateFromCollectorFile(files[1]));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[1]));
    
    // startFromBegining 
    cReader.startFromBegining();
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[0]));

  }

  private void readFile(int fileNum, int startIndex) throws Exception {
    int fileIndex = fileNum * 100 ;
    for (int i = startIndex; i < 100; i++) {
      String line = cReader.readLine();
      Assert.assertNotNull(line);
      Assert.assertEquals(new String(Base64.decodeBase64(line)),
          MessageUtil.constructMessage(fileIndex + i));
    }
    Assert.assertEquals(cReader.getCurrentFile().getName(), files[fileNum]);
  }

  @Test
  public void testReadFromStart() throws Exception {
    cReader = new CollectorStreamReader(partitionId, cluster, testStream, 10,
        10, true);
    cReader.build();
    cReader.initFromStart();
    cReader.openStream();
    readFile(0, 0);
    readFile(1, 0);
    readFile(2, 0);
    cReader.close();
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    cReader = new CollectorStreamReader(partitionId, cluster, testStream, 10,
        10, true);
    cReader.build();
    cReader.initializeCurrentFile(new PartitionCheckpoint(files[1], 20));
    cReader.openStream();

    readFile(1, 20);
    readFile(2, 0);
    cReader.close();
  }

  @Test
  public void testReadFromTimeStamp() throws Exception {
    cReader = new CollectorStreamReader(partitionId, cluster, testStream, 10,
        10, true);
    cReader.build();
    cReader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(files[1]));
    cReader.openStream();
    readFile(1, 0);
    readFile(2, 0);
    cReader.close();
  }

}
