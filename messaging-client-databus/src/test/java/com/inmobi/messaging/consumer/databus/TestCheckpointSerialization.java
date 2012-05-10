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
    PartitionId id1 = new PartitionId("cluster1", "collector1");
    PartitionId id2 = new PartitionId("cluster1", "collector2");
    PartitionId id3 = new PartitionId("cluster1", "collector3");
    PartitionCheckpoint pcp1 = new PartitionCheckpoint("file1", 100);
    PartitionCheckpoint pcp2 = new PartitionCheckpoint("file2", 100);
    partitionsChkPoint.put(id1, pcp1);
    partitionsChkPoint.put(id2, pcp2);
    partitionsChkPoint.put(id3, null);

    Checkpoint ckPoint1 = new Checkpoint(partitionsChkPoint);
    System.out.println("check point1: " + ckPoint1.toString());
    byte[] bytes = ckPoint1.toBytes();
    Checkpoint ckPoint2 = new Checkpoint(bytes);
    System.out.println("check point2: " + ckPoint2.toString());
    Assert.assertEquals(ckPoint1.toString(), ckPoint2.toString());
    Assert.assertEquals(ckPoint1, ckPoint2);
    Map<PartitionId, PartitionCheckpoint> map_cp2 = ckPoint2.getPartitionsCheckpoint();
    Assert.assertEquals(map_cp2.get(id1), pcp1);
    Assert.assertEquals(map_cp2.get(id2), pcp2);
    Assert.assertNull(map_cp2.get(id3));
  }
}
