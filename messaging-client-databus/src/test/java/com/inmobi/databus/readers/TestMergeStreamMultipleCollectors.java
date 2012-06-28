package com.inmobi.databus.readers;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestMergeStreamMultipleCollectors {

  private static final String testStream = "testclient";

  private String[] collectors = new String[] {"collector1", "collector2"};
  private static final String clusterName = "testCluster";
  private PartitionId partitionId = new PartitionId(clusterName, null);
  private MergedStreamReader reader;
  private Cluster cluster;
  private String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  Path[] databusFiles1 = new Path[3];
  Path[] databusFiles2 = new Path[3];

  @BeforeTest
  public void setup() throws Exception {
    // initialize config
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectors[0]), files, null,
        databusFiles1, 0, 3);
    TestUtil.setUpFiles(cluster, collectors[1], files, null, databusFiles2, 0,
        3);
  }

  private void readFile(int fileNum, int startIndex, Path filePath)
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


  @Test
  public void testReadFromStart() throws Exception {
    reader = new MergedStreamReader(partitionId, cluster,
        testStream, 1000, true);
    reader.build(CollectorStreamReader.getDateFromCollectorFile(files[0]));
    reader.initFromStart();
    Assert.assertNotNull(reader.getCurrentFile());
    reader.openStream();
    readFile(0, 0, databusFiles1[0]);
    readFile(0, 0, databusFiles2[0]);
    readFile(1, 0, databusFiles1[1]);
    readFile(1, 0, databusFiles2[1]);
    readFile(2, 0, databusFiles1[2]);
    readFile(2, 0, databusFiles2[2]);
    reader.close();
  }
}
