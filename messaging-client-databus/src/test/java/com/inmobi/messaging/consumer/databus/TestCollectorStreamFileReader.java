package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.SourceStream;

public class TestCollectorStreamFileReader {
  private static final String testStream = "testclient";

  private String collectorName = "collector1";
  private Path collectorDir;
  private String clusterName = "testCluster";
  private DatabusConfig databusConfig;
  private CollectorStreamFileReader cReader;
  private FileSystem fs;
  private Cluster cluster;
  private int msgIndex = 0;
  private PartitionId partitionId;
  private String file1 = testStream + "-2012-05-02-14-26_00000";
  private String file2 = testStream + "-2012-05-02-14-27_00000";
  private String file3 = testStream + "-2012-05-02-14-28_00000";
  private String[] files = new String[] {file1,file2, file3};
  private String file5 = collectorName + "-" + testStream + "-2012-05-02-14-24_00000.gz";

  
  @BeforeTest
  public void setup() throws Exception {
    // initialize config
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add(testStream);
    cluster = new Cluster(clusterName, 
      "/tmp/databus/" + this.getClass().getName(),
      "file:///", "local", null, sourceNames);

    partitionId = new PartitionId(clusterName, collectorName);
      
    // setup local stream dirs
    fs = FileSystem.get(cluster.getHadoopConf());
    Path streamDir = new Path(cluster.getDataDir(), testStream);
    this.collectorDir = new Path(streamDir, partitionId.getCollector());
    fs.mkdirs(collectorDir);

    // setup data dirs
    createMessageFile(file1);
    createMessageFile(file2);
    createMessageFile(file3);
  }
  
  @AfterTest
  public void cleanup() throws IOException {
    if (fs != null) {
      fs.delete(new Path(cluster.getRootDir()), true);
    }
  }
  
  private void createMessageFile(String fileName) throws Exception {
    Path path = new Path(collectorDir, fileName);
    FSDataOutputStream out = fs.create(path);
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
  
  @Test
  public void testInitialize() throws Exception {
    // Read from start
    cReader = new CollectorStreamFileReader(partitionId, cluster, testStream, 1000);
    cReader.initFromStart();
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir, file1));

    // Read from checkpoint with collector file name
    cReader.initializeCurrentFile(
      new PartitionCheckpoint(file2, 20));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir, file2));
    
    // Read from checkpoint with local stream file name
    cReader.initializeCurrentFile(new PartitionCheckpoint(
        LocalStreamFileReader.getLocalStreamFileName(collectorName, file2), 20));
    Assert.assertNull(cReader.getCurrentFile());

    //Read from startTime in collector dir
    cReader.initializeCurrentFile(CollectorStreamFileReader.getDateFromFile(file2));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir, file2));

    //Read from startTime in local stream directory 
    cReader.initializeCurrentFile(LocalStreamFileReader.getDateFromFile(file5));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir, file1));

  }
  
  private void readFile(int fileNum, int startIndex) throws Exception {
    int fileIndex = fileNum * 100 ;
    for (int i = startIndex; i < 100; i++) {
      String line = cReader.readLine();
      Assert.assertNotNull(line);
      Assert.assertEquals(new String(Base64.decodeBase64(line)),
          constructMessage(fileIndex + i));
    }
    Assert.assertEquals(cReader.getCurrentFile().getName(), files[fileNum]);
  }
  
  @Test
  public void testReadFromStart() throws Exception {
    cReader = new CollectorStreamFileReader(partitionId, cluster, testStream, 1000);
    cReader.initFromStart();
    cReader.openStream();
    readFile(0, 0);
    readFile(1, 0);
    readFile(2, 0);
    cReader.close();
  }
  
  @Test
  public void testReadFromCheckpoint() throws Exception {
    cReader = new CollectorStreamFileReader(partitionId, cluster, testStream, 1000);
    cReader.initializeCurrentFile(new PartitionCheckpoint(file2, 20));
    cReader.openStream();
    
    readFile(1, 20);
    readFile(2, 0);
    cReader.close();
  }

  @Test
  public void testReadFromTimeStamp() throws Exception {
    cReader = new CollectorStreamFileReader(partitionId, cluster,  testStream, 1000);
    cReader.initializeCurrentFile(
        CollectorStreamFileReader.getDateFromFile(file2));
    cReader.openStream();
    readFile(1, 0);
    readFile(2, 0);
    cReader.close();
  }

}
