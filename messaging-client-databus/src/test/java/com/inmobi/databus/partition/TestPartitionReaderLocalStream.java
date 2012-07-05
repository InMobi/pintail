package com.inmobi.databus.partition;

import java.io.IOException;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestPartitionReaderLocalStream extends TestAbstractClusterReader {
  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        databusFiles, 3);
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
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    super.testReadFromCheckpointWhichDoesNotExist();
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist2() throws Exception {
    super.testReadFromCheckpointWhichDoesNotExist2();
  }

  @Test
  public void testReadFromCheckpointWhichDoesNotExist3() throws Exception {
    super.testReadFromCheckpointWhichDoesNotExist3();
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
  boolean isLocal() {
    return true;
  }

}
