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
import com.inmobi.databus.utils.FileUtil;

public class TestLocalStreamFileReader {
  private static final String testStream = "testclient";

  private String collectorName = "collector1";
  private Path localStreamDir;
  private String clusterName = "testCluster";
  private DatabusConfig databusConfig;
  private LocalStreamFileReader lreader;
  private FileSystem fs;
  private Cluster cluster;
  private int msgIndex = 0;
  private PartitionId partitionId;
  private String file1 = collectorName + "-" + testStream + "-2012-05-02-14-26_00000.gz";
  private String file2 = collectorName + "-" + testStream + "-2012-05-02-14-27_00000.gz";
  private String file3 = collectorName + "-" + testStream + "-2012-05-02-14-28_00000.gz";
  private String[] files = new String[] {file1,file2, file3};
  private String file5 = testStream + "-2012-05-02-14-30_00000";

  
  @BeforeTest
  public void setup() throws Exception {
    // initialize config
      partitionId = new PartitionId(clusterName, collectorName);
      Set<String> sourceNames = new HashSet<String>();
      sourceNames.add(testStream);
      cluster = new Cluster(clusterName, 
        "/tmp/databus/" + this.getClass().getName(),
        "file:///", "local", null, sourceNames);

      
      // setup local stream dirs
      fs = FileSystem.get(cluster.getHadoopConf());
      localStreamDir = LocalStreamFileReader.getLocalStreamDir(cluster, testStream);
      fs.mkdirs(localStreamDir);

      // setup data dirs
      createMessageFile(file1);
      createMessageFile(file2);
      createMessageFile(file3);
  }
  
  @AfterTest
  public void cleanup() throws IOException {
    fs.delete(new Path(cluster.getRootDir()), true);
  }
  
  private void createMessageFile(String fileName) throws Exception {
    Path streamLocalDateDir = LocalStreamFileReader.getDateDir(cluster,
        testStream, fileName);
    Path targetFile = new Path(streamLocalDateDir, fileName);
    Path tmpPath = new Path(streamLocalDateDir, fileName + ".tmp");
    FSDataOutputStream out = fs.create(tmpPath);
    for (int i = 0; i < 100; i++) {
      out.write(Base64.encodeBase64(constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }
    out.close();
    FileUtil.gzip(tmpPath, targetFile, cluster.getHadoopConf());
    fs.delete(tmpPath, true);
  }
  
  private String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    str.append(index).append("Message");
    return str.toString();
  }
  
  @Test
  public void testInitialize() throws Exception {
    // Read from start
    lreader = new LocalStreamFileReader(partitionId, cluster, testStream);
    lreader.initFromStart();
    Assert.assertEquals(lreader.getCurrentFile(),
        new Path(LocalStreamFileReader.getDateDir(cluster, testStream, file1),
        file1));

    // Read from checkpoint with collector file name
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        CollectorStreamFileReader.getCollectorFileName(file2), 20));
    Assert.assertNull(lreader.getCurrentFile());
    
    // Read from checkpoint with local stream file name
    lreader.initializeCurrentFile(new PartitionCheckpoint(file2, 20));
    Assert.assertEquals(lreader.getCurrentFile(),
        new Path(LocalStreamFileReader.getDateDir(cluster, testStream, file2),
         file2));

    // Read from checkpoint with local stream file name, which does not exist
    lreader.initializeCurrentFile(new PartitionCheckpoint(
      LocalStreamFileReader.getLocalStreamFileName(collectorName, file5), 20));
    Assert.assertEquals(lreader.getCurrentFile(),
        new Path(LocalStreamFileReader.getDateDir(cluster, testStream, file1),
         file1));

    //Read from startTime in local stream directory 
    lreader.initializeCurrentFile(LocalStreamFileReader.getDateFromFile(file2));
    Assert.assertEquals(lreader.getCurrentFile(),
        new Path(LocalStreamFileReader.getDateDir(cluster, testStream, file2),
        file2));

    //Read from startTime in collector dir
    lreader.initializeCurrentFile(CollectorStreamFileReader.getDateFromFile(file5));
    Assert.assertNull(lreader.getCurrentFile());  

  }
  
  private void readFile(int fileNum, int startIndex) throws IOException {
    int fileIndex = fileNum * 100 ;
    for (int i = startIndex; i < 100; i++) {
      String line = lreader.readLine();
      Assert.assertNotNull(line);
      Assert.assertEquals(new String(Base64.decodeBase64(line)),
          constructMessage(fileIndex + i));
    }
    Assert.assertEquals(lreader.getCurrentFile().getName(), files[fileNum]);
  }
  
  @Test
  public void testReadFromStart() throws Exception {
    lreader = new LocalStreamFileReader(partitionId, cluster, testStream);
    lreader.initFromStart();
    lreader.openStream();
    
    readFile(0, 0);
    readFile(1, 0);
    readFile(2, 0);
  }
  
  @Test
  public void testReadFromCheckpoint() throws Exception {
    lreader = new LocalStreamFileReader(partitionId, cluster, testStream);
    lreader.initializeCurrentFile(new PartitionCheckpoint(file2, 20));
    lreader.openStream();
    
    readFile(1, 20);
    readFile(2, 0);
  }

  @Test
  public void testReadFromTimeStamp() throws Exception {
    lreader = new LocalStreamFileReader(partitionId, cluster,  testStream);
    lreader.initializeCurrentFile(
        LocalStreamFileReader.getDateFromFile(file2));
    lreader.openStream();
    readFile(1, 0);
    readFile(2, 0);
  }

}
