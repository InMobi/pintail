package com.inmobi.databus.readers;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestMergeStreamReader extends TestAbstractDatabusWaitingReader{

  @BeforeTest
  public void setup() throws Exception {
    // initialize config
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        databusFiles, 0, 3);
    conf = cluster.getHadoopConf();
    fs = FileSystem.get(conf);
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
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
  public void testReadFromTimeStamp() throws Exception {
    super.testReadFromTimeStamp();
  }

  @Override
  Path getStreamsDir() {
    return DatabusStreamReader.getStreamsDir(cluster, testStream);
  }

}
