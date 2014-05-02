package com.inmobi.messaging.consumer.databus;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2012 - 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.messaging.consumer.util.TestUtil;


public class TestCheckpointSerialization {

  @Test
  public void test() throws IOException {
    PartitionId id1 = new PartitionId("cluster1", "collector1");
    PartitionId id2 = new PartitionId("cluster1", "collector2");
    PartitionId id3 = new PartitionId("cluster1", "collector3");
    PartitionCheckpoint pcp1 = new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(TestUtil.files[0]), 100);
    PartitionCheckpoint pcp2 = new PartitionCheckpoint(
        CollectorStreamReader.getCollectorFile(TestUtil.files[1]), 100);

    Checkpoint ckPoint1 = new Checkpoint();
    ckPoint1.set(id1, pcp1);
    ckPoint1.set(id2, pcp2);
    ckPoint1.set(id3, null);
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
