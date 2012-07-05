package com.inmobi.databus.partition;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderWaitingLocalStream
    extends TestAbstractWaitingClusterReader {

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        databusFiles, 3, 0);
    fs = FileSystem.get(cluster.getHadoopConf());
  }

  void setupFiles(String[] files, Path[] newDatabusFiles) throws Exception {
    TestUtil.setUpFiles(cluster, collectorName, files, null, newDatabusFiles,
        files.length, 0);
  }

  @AfterTest
  public void cleanup() throws IOException {
    super.cleanup();
  }

  @Test
  public void testReadFromStart() throws Exception {
    super.testReadFromStart();
  }

  @Override
  boolean isLocal() {
    return true;
  }
}
