package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;

public class TestLocalStreamReader {
  private static final String testStream = "testclient";

  private static final String collectorName = "collector1";
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, collectorName);
  private LocalStreamReader lreader;
  private Cluster cluster;
  private String file1 = testStream + "-2012-05-02-14-26_00000.gz";
  private String file2 = testStream + "-2012-05-02-14-27_00000.gz";
  private String file3 =  testStream + "-2012-05-02-14-28_00000.gz";
  private String[] files = new String[] {file1,file2, file3};
  private String file5 = testStream + "-2012-05-02-14-30_00000";


  @BeforeTest
  public void setup() throws Exception {
    // initialize config
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, new String[] {file1, file2, file3}, null, 3);

  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    // Read from start
    lreader = new LocalStreamReader(partitionId, cluster, testStream);
    lreader.build();

    lreader.initFromStart();
    Assert.assertEquals(lreader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName, file1));

    // Read from checkpoint with collector file name
    lreader.initializeCurrentFile(new PartitionCheckpoint(file2, 20));
    Assert.assertNull(lreader.getCurrentFile());

    // Read from checkpoint with local stream file name
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName, file2), 20));
    Assert.assertEquals(lreader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName, file2));

    // Read from checkpoint with local stream file name, which does not exist
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName, file5), 20));
    Assert.assertEquals(lreader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName, file1));

    //Read from startTime in local stream directory 
    lreader.initializeCurrentFile(TestUtil.getDateFromCollectorFile(file2));
    Assert.assertEquals(lreader.getCurrentFile(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName, file2));

    //Read from startTime in collector dir
    lreader.initializeCurrentFile(TestUtil.getDateFromCollectorFile(file5));
    Assert.assertNull(lreader.getCurrentFile());  

  }

  private void readFile(int fileNum, int startIndex) throws Exception {
    int fileIndex = fileNum * 100 ;
    for (int i = startIndex; i < 100; i++) {
      String line = lreader.readLine();
      Assert.assertNotNull(line);
      Assert.assertEquals(new String(Base64.decodeBase64(line)),
          TestUtil.constructMessage(fileIndex + i));
    }
    Assert.assertEquals(lreader.getCurrentFile().getName(),
        TestUtil.getLocalStreamPath(cluster, testStream, collectorName,
            files[fileNum]).getName());
  }

  @Test
  public void testReadFromStart() throws Exception {
    lreader = new LocalStreamReader(partitionId, cluster, testStream);
    lreader.build();
    lreader.initFromStart();
    lreader.openStream();

    readFile(0, 0);
    readFile(1, 0);
    readFile(2, 0);
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    lreader = new LocalStreamReader(partitionId, cluster, testStream);
    lreader.build();
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName, file2), 20));
    lreader.openStream();

    readFile(1, 20);
    readFile(2, 0);
  }

  @Test
  public void testReadFromTimeStamp() throws Exception {
    lreader = new LocalStreamReader(partitionId, cluster,  testStream);
    lreader.build();
    lreader.initializeCurrentFile(TestUtil.getDateFromCollectorFile(file2));
    lreader.openStream();
    readFile(1, 0);
    readFile(2, 0);
  }

}
