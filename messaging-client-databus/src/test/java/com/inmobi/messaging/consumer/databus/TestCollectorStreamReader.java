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
  private String file1 = testStream + "-2012-05-02-14-26_00000";
  private String file2 = testStream + "-2012-05-02-14-27_00000";
  private String file3 = testStream + "-2012-05-02-14-28_00000";
  private String[] files = new String[] {file1,file2, file3};
  private String file5 = collectorName + "-" + testStream 
      + "-2012-05-02-14-24_00000.gz";


  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getName(), testStream,
        partitionId, new String[] {file1, file2, file3}, null, 0);
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
        file1));

    // Read from checkpoint with collector file name
    cReader.initializeCurrentFile(
        new PartitionCheckpoint(file2, 20));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        file2));

    // Read from checkpoint with local stream file name
    cReader.initializeCurrentFile(new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName, file2), 20));
    Assert.assertNull(cReader.getCurrentFile());

    //Read from startTime in collector dir
    cReader.initializeCurrentFile(TestUtil.getDateFromCollectorFile(file2));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        file2));

    //Read from startTime in local stream directory 
    cReader.initializeCurrentFile(TestUtil.getDateFromLocalStreamFile(file5));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        file1));

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
    cReader.initializeCurrentFile(new PartitionCheckpoint(file2, 20));
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
        TestUtil.getDateFromCollectorFile(file2));
    cReader.openStream();
    readFile(1, 0);
    readFile(2, 0);
    cReader.close();
  }

}
