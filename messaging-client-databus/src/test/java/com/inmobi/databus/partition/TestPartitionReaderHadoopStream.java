package com.inmobi.databus.partition;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.databus.DataEncodingType;
import com.inmobi.messaging.consumer.util.HadoopUtil;

public class TestPartitionReaderHadoopStream extends TestAbstractClusterReader {

  @BeforeTest
  public void setup() throws Exception {
    // setup fs
    files = new String[] {HadoopUtil.files[1], HadoopUtil.files[3],
        HadoopUtil.files[5]};
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

  @Test
  public void testInitialize() throws Exception {
    super.testInitialize();
  }

  @Test
  public void testReadFromStart() throws Exception {
    super.testReadFromStart();
  }

  @Test
  public void testReadFromCheckpoint() throws Exception {
    super.testReadFromCheckpoint();
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    super.testReadFromCheckpointWhichDoesNotExist();
  }

  @Test
  public void testReadFromStartTime() throws Exception {
    super.testReadFromStartTime();
  }

  @Test
  public void testReadFromStartTimeWithinStream() throws Exception {
    super.testReadFromStartTimeWithinStream();
  }

  @Test
  public void testReadFromStartTimeBeforeStream() throws Exception {
    super.testReadFromStartTimeBeforeStream();
  }

  @Test
  public void testReadFromStartTimeAfterStream() throws Exception {
    super.testReadFromStartTimeAfterStream();
  }

  @Override
  Path getStreamsDir() {
    return streamDir;
  }

  @Override
  boolean isDatabusData() {
    return false;
  }

}
