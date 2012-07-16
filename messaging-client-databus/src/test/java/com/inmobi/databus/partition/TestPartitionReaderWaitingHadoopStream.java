package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderWaitingHadoopStream extends TestAbstractWaitingClusterReader {

  protected static final String testStream = "testclient";
  protected static final String clusterName = "testCluster";
  protected PartitionId partitionId = new PartitionId(clusterName, null);

  protected LinkedBlockingQueue<QueueEntry> buffer = 
      new LinkedBlockingQueue<QueueEntry>(150);
  protected PartitionReader preader;

  protected String[] files; 

  protected Path[] databusFiles = new Path[3];

  protected final String collectorName = "collector1";

  FileSystem fs;
  Path streamDir;
  Configuration conf = new Configuration();
  String inputFormatClass;
  DataEncodingType dataEncoding;

  @BeforeTest
  public void setup() throws Exception {
    files = new String[] {HadoopUtil.files[1],
        HadoopUtil.files[3], HadoopUtil.files[5]};
    newFiles = new String[] {HadoopUtil.files[6],
        HadoopUtil.files[7], HadoopUtil.files[8] };
    // setup fs
    fs = FileSystem.getLocal(conf);
    streamDir = new Path("/tmp/test/hadoop/" + this.getClass().getSimpleName(),
         testStream).makeQualified(fs);
    HadoopUtil.setupHadoopCluster(conf, files, null, databusFiles, streamDir);
    inputFormatClass = SequenceFileInputFormat.class.getName();
    dataEncoding = DataEncodingType.NONE;
  }

  @AfterTest
  public void cleanup() throws IOException {
    fs.delete(streamDir.getParent(), true);
  }

  @Override
  void setupFiles(String[] files, Path[] newDatabusFiles) throws Exception {
    HadoopUtil.setUpHadoopFiles(streamDir, conf, files, null, newDatabusFiles);    
  }

  @Override
  boolean isDatabusData() {
    return false;
  }
}
