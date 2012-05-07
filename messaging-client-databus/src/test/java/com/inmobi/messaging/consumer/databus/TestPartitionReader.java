package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.utils.FileUtil;

public class TestPartitionReader {
  private static final String testStream = "testclient";
  private BlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);

  private String collectorName = "collector1";
  private String clusterName = "testCluster";
  private FileSystem fs;
  private Cluster cluster;
  private Path collectorDir;
  private int msgIndex = 0;
  private PartitionReader preader;
  private PartitionId partitionId;
  private String doesNotExist1 =  testStream + "-2012-05-02-14-24_00000";
  private String file1 =  testStream + "-2012-05-02-14-26_00000";
  private String file2 =  testStream + "-2012-05-02-14-27_00000";
  private String file3 =  testStream + "-2012-05-02-14-28_00000";
  private String file4 =  testStream + "-2012-05-02-14-29_00000";
  private String file41 = testStream + "-2012-05-02-14-30_00000";
  private String file5 =  testStream + "-2012-05-02-14-31_00000";
  private String file51 = testStream + "-2012-05-02-14-32_00000";
  private String file6 =  testStream + "-2012-05-02-14-33_00000";
  private String file61 = testStream + "-2012-05-02-14-34_00000";
  private String doesNotExist2 =  testStream + "-2012-05-02-14-35_00000";
  
  @BeforeTest
  public void setup() throws Exception {
    // initialize config
      Set<String> sourceNames = new HashSet<String>();
      sourceNames.add(testStream);
      cluster = new Cluster(clusterName, 
        "/tmp/databus/" + this.getClass().getName(),
        "file:///", "local", null, sourceNames);
      Path streamDir = new Path(cluster.getDataDir(), testStream);

      partitionId = new PartitionId(clusterName, collectorName);
      
      // setup stream and collector dirs
      fs = FileSystem.get(cluster.getHadoopConf());
      collectorDir = new Path(streamDir, collectorName);
      fs.delete(collectorDir, true);
      fs.mkdirs(collectorDir);

      // setup data dirs
      createMessageFile(file1);
      createMessageFile(file2);
      createMessageFile(file3);
      createMessageFile(file4);
      createMessageFile(file5);
      createMessageFile(file6);
      
      // create empty files
      createEmptyFile(file41);
      createEmptyFile(file51);
      createEmptyFile(file61);
      
      fs.delete(new Path(cluster.getLocalFinalDestDirRoot()), true);
      moveFileToStreamLocal(file1);
      moveFileToStreamLocal(file2);    
      moveFileToStreamLocal(file3);    
  }
  
  @AfterTest
  public void cleanup() throws IOException {
    fs.delete(new Path(cluster.getRootDir()), true);
  }
  
  private void createMessageFile(String fileName) throws IOException {
    FSDataOutputStream out = fs.create(new Path(collectorDir, fileName));
    for (int i = 0; i < 100; i++) {
      out.write(Base64.encodeBase64(constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }
    out.close();
  }

  private void createEmptyFile(String fileName) throws IOException {
    FSDataOutputStream out = fs.create(new Path(collectorDir, fileName));
    out.close();
  }

  private String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    str.append(index).append("Message");
    return str.toString();
  }
  
  private void assertBuffer(String fileName, int fileNum, int startIndex)
      throws InterruptedException {
    int fileIndex = (fileNum - 1) * 100 ;
    for (int i = startIndex; i < 100; i++) {
      QueueEntry entry = buffer.take();
      Assert.assertEquals(entry.partitionId, partitionId);
      Assert.assertEquals(entry.partitionChkpoint,
          new PartitionCheckpoint(fileName, i + 1));
      Assert.assertEquals(new String(entry.message.getData().array()),
        constructMessage(fileIndex + i));
    }
  }

  @Test
  public void testInitialize() throws Exception {
    // Read from start
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(null, -1), cluster, buffer, testStream, null, 1000);
    Assert.assertEquals(preader.getCurrentFile(),
        new Path(CollectorStreamReader.getDateDir(cluster, testStream, file1),
        LocalStreamReader.getLocalStreamFileName(collectorName, file1)));

    // Read from checkpoint with collector file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(file2, 20), cluster, buffer, testStream, null, 1000);
    Assert.assertEquals(preader.getCurrentFile(),
        new Path(CollectorStreamReader.getDateDir(cluster, testStream, file2),
        LocalStreamReader.getLocalStreamFileName(collectorName, file2)));

    // Read from checkpoint with local stream file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(LocalStreamReader.getLocalStreamFileName(
          collectorName, file2), 20),
        cluster, buffer, testStream, null, 1000);
    Assert.assertEquals(preader.getCurrentFile(),
        new Path(CollectorStreamReader.getDateDir(cluster, testStream, file2),
        LocalStreamReader.getLocalStreamFileName(collectorName, file2)));

    // Read from checkpoint with collector file name
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(file5, 20), cluster, buffer, testStream, null, 1000);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir, file5));    

    //Read from startTime in local stream directory 
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(null, -1), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromFile(file2), 1000);
    Assert.assertEquals(preader.getCurrentFile(),
        new Path(CollectorStreamReader.getDateDir(cluster, testStream, file2),
        LocalStreamReader.getLocalStreamFileName(collectorName, file2)));

    //Read from startTime in collector dir
    preader = new PartitionReader(partitionId,
        new PartitionCheckpoint(file1, 10), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromFile(file5), 1000);
    Assert.assertEquals(preader.getCurrentFile(), new Path(collectorDir, file5));    

  }
  
  @Test
  public void testReadFromStart() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        null, -1), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file1), 1, 0);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file2), 2,  0);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file3), 3,  0);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    assertBuffer(file4, 4,  0);
    assertBuffer(file5, 5,  0);
    assertBuffer(file6, 6,  0);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }
  
  @Test
  public void testReadFromCheckpointWithCollectorFileName() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file2, 20), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, file2), 2,  20);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, file3), 3,  0);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    assertBuffer(file4, 4,  0);
    assertBuffer(file5, 5,  0);
    assertBuffer(file6, 6,  0);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWithLocalStreamFileName() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName, file2), 20),
        cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, file2), 2,  20);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, file3), 3,  0);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    assertBuffer(file4, 4,  0);
    assertBuffer(file5, 5,  0);
    assertBuffer(file6, 6,  0);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWithCollectorFile() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file5, 40), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    assertBuffer(file5, 5,  40);
    assertBuffer(file6, 6,  0);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWithCollectorFileWhichDoesNotExist()
      throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        doesNotExist2, 40), cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file1), 1, 0);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file2), 2,  0);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file3), 3,  0);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    assertBuffer(file4, 4,  0);
    assertBuffer(file5, 5,  0);
    assertBuffer(file6, 6,  0);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromCheckpointWithLocalStreamFileWhichDoesNotExist()
      throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        LocalStreamReader.getLocalStreamFileName(collectorName,
          doesNotExist1), 20),
        cluster, buffer, testStream, null, 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file1), 1, 0);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file2), 2,  0);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file3), 3,  0);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    assertBuffer(file4, 4,  0);
    assertBuffer(file5, 5,  0);
    assertBuffer(file6, 6,  0);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeInLocalStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file1, 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromFile(file2), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, file2), 2,  0);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(
        collectorName, file3), 3,  0);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    assertBuffer(file4, 4,  0);
    assertBuffer(file5, 5,  0);
    assertBuffer(file6, 6,  0);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeInCollectorStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file1, 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromFile(file5), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    assertBuffer(file5, 5,  0);
    assertBuffer(file6, 6,  0);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file2, 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromFile(doesNotExist1), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        LocalStreamReader.class.getName());
    preader.execute();
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file1), 1, 0);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file2), 2,  0);
    assertBuffer(LocalStreamReader.getLocalStreamFileName(collectorName,
        file3), 3,  0);
    Assert.assertEquals(preader.getCurrentReader().getClass().getName(),
        CollectorStreamReader.class.getName());
    preader.execute();
    assertBuffer(file4, 4,  0);
    assertBuffer(file5, 5,  0);
    assertBuffer(file6, 6,  0);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    preader = new PartitionReader(partitionId, new PartitionCheckpoint(
        file2, 20), cluster, buffer, testStream,
        CollectorStreamReader.getDateFromFile(doesNotExist2), 1000);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNull(preader.getCurrentReader());
  }

  private void moveFileToStreamLocal(String file) throws Exception {
    String localStreamFileName = LocalStreamReader.getLocalStreamFileName(collectorName, file);
    Path streamLocalDateDir = CollectorStreamReader.getDateDir(cluster, testStream, file);
    System.out.println("stream local dir:" + streamLocalDateDir);
    Path targetFile = new Path(streamLocalDateDir, localStreamFileName);
    Path collectorPath = new Path(collectorDir, file);
    FileUtil.gzip(collectorPath, targetFile, cluster.getHadoopConf());
    fs.delete(collectorPath, true);
  }
}
