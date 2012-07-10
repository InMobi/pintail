package com.inmobi.databus.readers;

import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.Assert;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.util.HadoopUtil;
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
  protected Path[] finalFiles = new Path[3];
  protected FileSystem fs;
  protected Configuration conf;
  protected Path streamDir;

  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  abstract Path getStreamsDir();

  public void testInitialize() throws Exception {
    // Read from start
    lreader = new DatabusStreamWaitingReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        streamDir, TextInputFormat.class.getCanonicalName(), conf, 1000,
        false);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[0].getParent()));
    lreader.build(cal.getTime());

    lreader.initFromStart();
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[0]);

    // Read from checkpoint with stream file name
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        DatabusStreamWaitingReader.getHadoopStreamFile(
            fs.getFileStatus(finalFiles[1])), 20));
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[1]);

    // Read from checkpoint with stream file name, which does not exist
    lreader.initializeCurrentFile(new PartitionCheckpoint(
        HadoopUtil.getOlderFile(streamDir, fs, finalFiles[0]), 20));
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[0]);

    //Read from startTime in stream directory, before the stream
    cal.add(Calendar.MINUTE, -2);
    lreader.initializeCurrentFile(cal.getTime());
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[0]);

    //Read from startTime within stream directory
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[1].getParent()));
    lreader.initializeCurrentFile(cal.getTime());
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[1]);

    //Read from startTime within the stream, but no min directory
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[0].getParent()));
    cal.add(Calendar.MINUTE, 1);
    lreader.initializeCurrentFile(cal.getTime());
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[1]);

    //Read from startTime in after the stream
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[2].getParent()));
    cal.add(Calendar.MINUTE, 2);
    lreader.initializeCurrentFile(cal.getTime());
    Assert.assertNull(lreader.getCurrentFile());  

    // startFromNextHigher with filename
    lreader.startFromNextHigher(fs.getFileStatus(finalFiles[1]));
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[2]);

    // startFromTimestamp with date
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[1].getParent()));
    lreader.startFromTimestmp(cal.getTime());
    Assert.assertEquals(lreader.getCurrentFile(), finalFiles[1]);
    
    // startFromBegining 
   lreader.startFromBegining();
   Assert.assertEquals(lreader.getCurrentFile(), finalFiles[0]);
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
        getStreamsDir(), TextInputFormat.class.getCanonicalName(), conf, 1000,
        false);
    lreader.build(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[0].getParent()));
    lreader.initFromStart();
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(lreader, 0, 0, finalFiles[0]);
    readFile(lreader, 1, 0, finalFiles[1]);
    readFile(lreader, 2, 0, finalFiles[2]);
    lreader.close();
  }

  public void testReadFromCheckpoint() throws Exception {
    lreader = new DatabusStreamWaitingReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        getStreamsDir(), TextInputFormat.class.getCanonicalName(), conf, 1000,
        false);
    PartitionCheckpoint pcp = new PartitionCheckpoint(
        DatabusStreamWaitingReader.getHadoopStreamFile(
            fs.getFileStatus( finalFiles[1])), 20);
    lreader.build(DatabusStreamWaitingReader.getBuildTimestamp(streamDir, pcp));
    lreader.initializeCurrentFile(pcp);
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(lreader, 1, 20, finalFiles[1]);
    readFile(lreader, 2, 0, finalFiles[2]);
    lreader.close();
  }

  public void testReadFromTimeStamp() throws Exception {
    lreader = new DatabusStreamWaitingReader(partitionId,
        FileSystem.get(cluster.getHadoopConf()), testStream,
        getStreamsDir(), TextInputFormat.class.getCanonicalName(), conf, 1000,
        false);
    lreader.build(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[1].getParent()));
    lreader.initializeCurrentFile(
        DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[1].getParent()));
    Assert.assertNotNull(lreader.getCurrentFile());
    lreader.openStream();
    readFile(lreader, 1, 0, finalFiles[1]);
    readFile(lreader, 2, 0, finalFiles[2]);
    lreader.close();
  }

}
