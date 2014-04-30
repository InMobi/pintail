package com.inmobi.databus.partition;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2014 InMobi
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

import java.util.Map;
import java.util.TreeMap;

import com.inmobi.messaging.consumer.databus.MessageCheckpoint;

/**
 * Checkpoint for the segments of databus stream consumer.
 * this class is used to construct the checkpoint list for partition. Partition
 * checkpoint list contains the segment ids and respective partition checkpoints.
 */
public class PartitionCheckpointList implements MessageCheckpoint {

  // map of static id to its checkpoint
  private final Map<Integer, PartitionCheckpoint> pChkpoints =
      new TreeMap<Integer, PartitionCheckpoint>();

  public PartitionCheckpointList(Map<Integer, PartitionCheckpoint> chkpoints) {
    this.pChkpoints.putAll(chkpoints);
  }

  public PartitionCheckpointList() {
  }

  public Map<Integer, PartitionCheckpoint> getCheckpoints() {
    return pChkpoints;
  }

  public void set(int segmentId, PartitionCheckpoint pck) {
    pChkpoints.put(segmentId, pck);
  }

  /**
   * Checks the partition checkpoint list is empty or not..
   * @return false if the partition checkpoint list contains at least one entry.
   */
  @Override
  public boolean isNULL() {
    if (!pChkpoints.isEmpty()) {
      for (Map.Entry<Integer, PartitionCheckpoint> entry
          : pChkpoints.entrySet()) {
        if (entry.getValue() != null) {
          return false;
        }
      }
    }
    return true;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    for (Map.Entry<Integer, PartitionCheckpoint> entry : pChkpoints
        .entrySet()) {
      buf.append(entry.getKey().toString())
      .append(":");
      if (entry.getValue() != null) {
        buf.append(entry.getValue().toString());
      } else {
        buf.append("null");
      }
      buf.append(", ");
    }
    return buf.toString();
  }
}
