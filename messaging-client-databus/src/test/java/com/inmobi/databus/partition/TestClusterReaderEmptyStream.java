package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestClusterReaderEmptyStream {

  private static final String testStream = "testclient";

  private LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(1000);
  private PartitionReader preader;
  private static final String clusterName = "testCluster";
  private PartitionId clusterId = new PartitionId(clusterName, null);

  FileSystem fs;
  Path streamDir;
  Configuration conf = new Configuration();
  String inputFormatClass;

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    fs = FileSystem.getLocal(conf);
    streamDir = new Path("/tmp/test/hadoop/" + this.getClass().getSimpleName(),
         testStream).makeQualified(fs);
    HadoopUtil.setupHadoopCluster(conf, null, null, streamDir);
    inputFormatClass = SequenceFileInputFormat.class.getName();
  }

  @AfterTest
  public void cleanup() throws IOException {
    fs.delete(streamDir.getParent(), true);
  }

  @Test
  public void testInitialize() throws Exception {

    // Read from start time 
    preader = new PartitionReader(clusterId, null, fs, buffer,
        streamDir, conf, inputFormatClass, CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[0]), 
        1000,
        DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((AbstractPartitionStreamReader)preader
        .getReader()).getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());

    //Read from checkpoint
    preader = new PartitionReader(clusterId, new PartitionCheckpoint(
        new HadoopStreamFile(DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
            CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[0])),
            "dummyfile", 0L), 20), fs, buffer,
        streamDir, conf, inputFormatClass, null, 
        1000, DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((AbstractPartitionStreamReader)preader
        .getReader()).getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());

    //Read from startTime with checkpoint
    preader = new PartitionReader(clusterId, new PartitionCheckpoint(
        new HadoopStreamFile(DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
            CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[0])),
            "dummyfile", 0L), 20), fs, buffer,
        streamDir, conf, inputFormatClass, CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[0]), 
        1000,
        DataEncodingType.BASE64, true);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((AbstractPartitionStreamReader)preader
        .getReader()).getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
  }
  
}
