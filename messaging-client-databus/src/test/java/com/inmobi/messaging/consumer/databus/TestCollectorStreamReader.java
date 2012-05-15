package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;

public class TestCollectorStreamReader {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);
  private Path collectorDir;
  private CollectorStreamReader cReader;
  private Cluster cluster;
  private String[] files = new String[] {TestUtil.files[0], TestUtil.files[1],
      TestUtil.files[2]};
  private String file5 = collectorName + "-" + testStream 
      + "-2012-05-02-14-24_00000.gz";


  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, 0);
    collectorDir = new Path(new Path(cluster.getDataDir(), testStream),
        collectorName);
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    // Read from start
    cReader = new CollectorStreamReader(partitionId, cluster, testStream, 1000);
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
        LocalStreamReader.getLocalStreamFileName(collectorName, files[1]), 20));
    Assert.assertNull(cReader.getCurrentFile());

    //Read from startTime in collector dir
    cReader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(files[1]));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[1]));

    //Read from startTime in local stream directory 
    cReader.initializeCurrentFile(LocalStreamReader.getDateFromLocalStreamFile(
        testStream, collectorName, file5));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[0]));

  }

  private void readFile(int fileNum, int startIndex) throws Exception {
    int fileIndex = fileNum * 100 ;
    for (int i = startIndex; i < 100; i++) {
      String line = cReader.readLine();
      Assert.assertNotNull(line);
      Assert.assertEquals(new String(Base64.decodeBase64(line)),
          TestUtil.constructMessage(fileIndex + i));
    }
    Assert.assertEquals(cReader.getCurrentFile().getName(), files[fileNum]);
  }

  @Test
  public void testReadFromStart() throws Exception {
    cReader = new CollectorStreamReader(partitionId, cluster, testStream, 1000);
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
    cReader = new CollectorStreamReader(partitionId, cluster, testStream, 1000);
    cReader.build();
    cReader.initializeCurrentFile(new PartitionCheckpoint(files[1], 20));
    cReader.openStream();

    readFile(1, 20);
    readFile(2, 0);
    cReader.close();
  }

  @Test
  public void testReadFromTimeStamp() throws Exception {
    cReader = new CollectorStreamReader(partitionId, cluster,  testStream,
        1000);
    cReader.build();
    cReader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(files[1]));
    cReader.openStream();
    readFile(1, 0);
    readFile(2, 0);
    cReader.close();
  }

}
