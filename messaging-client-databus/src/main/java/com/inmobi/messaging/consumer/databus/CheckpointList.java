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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.inmobi.databus.partition.DeltaPartitionCheckPoint;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.checkpoint.CheckpointProvider;

/**
 * Checkpoint for the segments of databus stream consumer.
 * This class is used to construct the checkpoint list. Checkpoint list contains
 * set of segment ids and respective checkpoints.
 * This class also implements methods for writing the consumer checkpoint to the
 * file system and to read the consumer checkpoint from the file system.
 */
public class CheckpointList implements ConsumerCheckpoint {

  // map of static id to its checkpoint
  private final Map<Integer, Checkpoint> chkpoints =
      new TreeMap<Integer, Checkpoint>();
  private final Set<Integer> idList;

  public CheckpointList(Set<Integer> idList) {
    this.idList = idList;
  }

  public Map<Integer, Checkpoint> getCheckpoints() {
    return chkpoints;
  }

  @Override
  public void set(PartitionId pid, MessageCheckpoint msgCkp) {
    DeltaPartitionCheckPoint checkPoint = (DeltaPartitionCheckPoint) msgCkp;
    for (Map.Entry<Integer, PartitionCheckpoint> entry :
      checkPoint.getDeltaCheckpoint().entrySet()) {
      setConsumerCheckpoint(pid, entry.getKey(), entry.getValue());
    }
  }

  private void setConsumerCheckpoint(PartitionId pid, Integer minute,
      PartitionCheckpoint pck) {
    Checkpoint tmpChkPoint = chkpoints.get(minute);
    if (tmpChkPoint == null) {
      tmpChkPoint = new Checkpoint();
    }
    tmpChkPoint.set(pid, pck);
    chkpoints.put(minute, tmpChkPoint);
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    for (Map.Entry<Integer, Checkpoint> entry : chkpoints
        .entrySet()) {
      buf.append(entry.getKey().toString())
      .append(":");
      if (entry.getValue() != null) {
        buf.append(entry.getValue().toString());
      } else {
        buf.append("null");
      }
      buf.append("\n ");
    }
    return buf.toString();
  }

  public static String getChkpointKey(String superKey, int id) {
    return superKey + "_" + id;
  }

  public void write(CheckpointProvider checkpointProvider, String superKey)
      throws IOException {
    for (Map.Entry<Integer, Checkpoint> entry : chkpoints.entrySet()) {
      entry.getValue().write(checkpointProvider,
          getChkpointKey(superKey, entry.getKey()));
    }
  }

  /**
   * It constructs a partition checkpoint list for the given partition
   * from the checkpoint list(consumer checkpoint).
   */
  public PartitionCheckpointList preaprePartitionCheckPointList(PartitionId pid) {
    PartitionCheckpointList partitionCheckpointList = new PartitionCheckpointList();
    for (Map.Entry<Integer, Checkpoint> entry : this.getCheckpoints().
        entrySet()) {
      partitionCheckpointList.set(entry.getKey(),
          entry.getValue().getPartitionsCheckpoint().get(pid));
    }
    return partitionCheckpointList;
  }

  public void read(CheckpointProvider checkpointProvider, String superKey)
      throws IOException {
    for (Integer id : idList) {
      Checkpoint checkpoint = new Checkpoint();
      checkpoint.read(checkpointProvider, getChkpointKey(superKey, id));
      chkpoints.put(id, checkpoint);
    }
  }

  @Override
  public void clear() {
    chkpoints.clear();
  }

  public void migrateCheckpoint(Map<PartitionId, PartitionId> defaultAndNewPidMap) {
    boolean migrateRequired = false;
    for (Map.Entry<Integer, Checkpoint> entry : chkpoints.entrySet()) {
      Checkpoint checkpoint = chkpoints.get(entry.getKey());
      if (!migrateRequired) {
        for (PartitionId pid : checkpoint.getPartitionsCheckpoint().keySet()) {
          if (defaultAndNewPidMap.containsKey(pid)) {
            migrateRequired = true;
            break;
          }
        }
        if (!migrateRequired) {
          break;
        }
      }
      Checkpoint newCheckpoint = new Checkpoint();
      for (Map.Entry<PartitionId, PartitionCheckpoint> partitionCkEntry :
        checkpoint.getPartitionsCheckpoint().entrySet()) {
        PartitionId defaultPid = partitionCkEntry.getKey();
        if (defaultAndNewPidMap.containsKey(defaultPid)) {
          PartitionCheckpoint pck = partitionCkEntry.getValue();
          PartitionId newPid = defaultAndNewPidMap.get(defaultPid);
          newCheckpoint.set(newPid, pck);
        }
      }
      chkpoints.put(entry.getKey(), newCheckpoint);
    }
  }
}
