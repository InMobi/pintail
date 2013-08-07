package com.inmobi.databus.readers;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.Message;

import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

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
  private Configuration conf;
  int consumerNumber;
  String fsUri;
  private String testRootDir;

  @BeforeTest
  public void setup() throws Exception {
    InputStream inputStream = this.getClass().getClassLoader().
        getResourceAsStream("rootdir.properties");
    testRootDir = TestUtil.getConfiguredRootDir(inputStream, "/tmp/test/");
    consumerNumber = 1;
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, partitionId, files, null, 0);
    collectorDir = new Path(new Path(cluster.getDataDir(), testStream),
        collectorName);
    conf = cluster.getHadoopConf();
    FileSystem fs = FileSystem.get(conf);
    fsUri = fs.getUri().toString();
    TestUtil.createEmptyFile(fs, collectorDir, testStream + "_current");
    TestUtil.createEmptyFile(fs, collectorDir, "scribe_stats");
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Test
  public void testInitialize() throws Exception {
    CollectorReaderStatsExposer metrics = new
        CollectorReaderStatsExposer(testStream, "c1", partitionId.toString(),
            consumerNumber, fsUri);
    // Read from start
    cReader = new CollectorStreamReader(partitionId, FileSystem.get(
        cluster.getHadoopConf()), testStream,
        CollectorStreamReader.getCollectorDir(cluster, testStream, collectorName),
        10, 10, metrics, conf, true, null, true);
    cReader.build();
    cReader.initFromStart();
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[0]));

    // Read from checkpoint with collector file name
    cReader.initializeCurrentFile(new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20));
    Assert.assertEquals(cReader.getCurrentFile(), new Path(collectorDir,
        files[1]));

    // Read from checkpoint with collector file name which does not exist
    // and is before the stream
    cReader.initializeCurrentFile(new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist1), 20));
    Assert.assertNull(cReader.getCurrentFile());

    // Read from checkpoint with collector file name which does not exist
    // and is within the stream
    cReader.initializeCurrentFile(new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist2), 20));
    Assert.assertNull(cReader.getCurrentFile());

    // Read from checkpoint with collector file name which does not exist
    // is after the stream
    cReader.initializeCurrentFile(new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(doesNotExist3), 20));
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
      Message line = cReader.readLine();
      Assert.assertNotNull(line);
      Assert.assertEquals(new String(line.getData().array()),
          MessageUtil.constructMessage(fileIndex + i));
    }
    Assert.assertEquals(cReader.getCurrentFile().getName(), files[fileNum]);
  }

  @Test
  public void testReadFromStart() throws Exception {
    CollectorReaderStatsExposer metrics = new
        CollectorReaderStatsExposer(testStream, "c1", partitionId.toString(),
            consumerNumber, fsUri);
    cReader = new CollectorStreamReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        CollectorStreamReader.getCollectorDir(cluster, testStream, collectorName),
        10, 10, metrics, conf, true, null, true);
    cReader.build();
    cReader.initFromStart();
    cReader.openStream();
    readFile(0, 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 100);
    readFile(1, 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 200);
    readFile(2, 0);
    cReader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(metrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(metrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(metrics.getListOps() > 0);
    Assert.assertTrue(metrics.getOpenOps() > 0);
    Assert.assertTrue(metrics.getFileStatusOps() == 0);
    Assert.assertTrue(metrics.getExistsOps() > 0);
    Assert.assertTrue(metrics.getNumberRecordReaders() == 0);
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    CollectorReaderStatsExposer metrics = new
        CollectorReaderStatsExposer(testStream, "c1", partitionId.toString(),
            consumerNumber, fsUri);
    cReader = new CollectorStreamReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        CollectorStreamReader.getCollectorDir(cluster, testStream, collectorName),
        10, 10, metrics, conf, true, null, true);
    cReader.build();
    cReader.initializeCurrentFile(new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(files[1]), 20));
    cReader.openStream();

    readFile(1, 20);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 80);
    readFile(2, 0);
    cReader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 180);
    Assert.assertEquals(metrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(metrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(metrics.getListOps() > 0);
    Assert.assertTrue(metrics.getOpenOps() > 0);
    Assert.assertTrue(metrics.getFileStatusOps() == 0);
    Assert.assertTrue(metrics.getExistsOps() > 0);
    Assert.assertTrue(metrics.getNumberRecordReaders() == 0);
  }

  @Test
  public void testReadFromTimeStamp() throws Exception {
    CollectorReaderStatsExposer metrics = new 
        CollectorReaderStatsExposer(testStream, "c1", partitionId.toString(),
            consumerNumber, fsUri);
    cReader = new CollectorStreamReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        CollectorStreamReader.getCollectorDir(cluster, testStream, collectorName),
        10, 10, metrics, conf, true, null, true);
    cReader.build();
    cReader.initializeCurrentFile(
        CollectorStreamReader.getDateFromCollectorFile(files[1]));
    cReader.openStream();
    readFile(1, 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 100);
    readFile(2, 0);
    cReader.close();
    Assert.assertEquals(metrics.getHandledExceptions(), 0);
    Assert.assertEquals(metrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(metrics.getWaitTimeInSameFile(), 0);
    Assert.assertEquals(metrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(metrics.getListOps() > 0);
    Assert.assertTrue(metrics.getOpenOps() > 0);
    Assert.assertTrue(metrics.getFileStatusOps() == 0);
    Assert.assertTrue(metrics.getExistsOps() > 0);
    Assert.assertTrue(metrics.getNumberRecordReaders() == 0);
  }

}
