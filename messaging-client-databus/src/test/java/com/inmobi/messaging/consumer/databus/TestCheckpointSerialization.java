package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCheckpointSerialization {

  @Test
  public void test() throws IOException {
    Map<PartitionId, PartitionCheckpoint> partitionsChkPoint =
        new HashMap<PartitionId, PartitionCheckpoint>();
    PartitionId id = new PartitionId("cluster1", "collector1");
    partitionsChkPoint.put(id, new PartitionCheckpoint(id, "file1", 100));
    Checkpoint ckPoint = new Checkpoint(partitionsChkPoint);
    byte[] bytes = ckPoint.toBytes();
    Assert.assertEquals(ckPoint, new Checkpoint(bytes));

  }
}
