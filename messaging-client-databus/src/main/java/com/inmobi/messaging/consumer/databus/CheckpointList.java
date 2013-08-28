package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashMap;
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
  private Map<Integer, Checkpoint> chkpoints =
      new TreeMap<Integer, Checkpoint>();
  private final Set<Integer> idList;

  public CheckpointList(Set<Integer> idList) {
    this.idList = idList;
  }

  void setCheckpoint(Map<Integer, Checkpoint> chkpoints) {
    this.chkpoints = chkpoints;
  }

  public  Map<Integer, Checkpoint> getCheckpoints() {
    return chkpoints;
  }

  @Override
  public void set(PartitionId pid, MessageCheckpoint msgCkp) {
    DeltaPartitionCheckPoint checkPoint = (DeltaPartitionCheckPoint) msgCkp;
    for (Map.Entry<Integer, PartitionCheckpoint> entry :
      checkPoint.getDeltaCheckpoint().entrySet()) {
      Integer minute = entry.getKey();
      PartitionCheckpoint pck = entry.getValue();
      setConsumerCheckpoint(pid, minute, pck);
    }
  }

  private void setConsumerCheckpoint(PartitionId pid, Integer minute,
      PartitionCheckpoint pck) {
    Map<PartitionId, PartitionCheckpoint> tmpPckMap =
        new HashMap<PartitionId, PartitionCheckpoint>();
    Checkpoint tmpChkPoint = chkpoints.get(minute);
    if (tmpChkPoint == null) {
      tmpPckMap = new HashMap<PartitionId, PartitionCheckpoint>();
      tmpPckMap.put(pid, pck);
      tmpChkPoint = new Checkpoint(tmpPckMap);
    } else {
      tmpChkPoint.set(pid, pck);
    }
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
      buf.append(", ");
    }
    return buf.toString();
  }

  public static String getChkpointKey(String superKey, int id) {
    return superKey + "_" + id;
  }

  public void write(CheckpointProvider checkpointProvider, String superKey)
      throws IOException {
    try {
      for (Map.Entry<Integer, Checkpoint> entry : chkpoints.entrySet()) {
        checkpointProvider.checkpoint(getChkpointKey(superKey, entry.getKey()),
            entry.getValue().toBytes());
      }
    } catch (Exception e) {
      throw new IOException("Could not checkpoint fully. It might be partial. ",
          e);
    }
  }

  /**
   * It constructs a partition checkpoint list for the given partition
   * from the checkpoint list(consumer checkpoint).
   */
  public void preaprePartitionCheckPointList(PartitionId pid,
      PartitionCheckpointList partitionCheckpointList) {
    PartitionCheckpoint partitionCheckpoint;
    if (!this.getCheckpoints().isEmpty()) {
      for (Map.Entry<Integer, Checkpoint> entry : this.getCheckpoints().
          entrySet()) {
        Checkpoint cp = entry.getValue();
        if (cp.getPartitionsCheckpoint().containsKey(pid)) {
          partitionCheckpoint = cp.getPartitionsCheckpoint().get(pid);
          partitionCheckpointList.set(entry.getKey(), partitionCheckpoint);
        }
      }
    }
  }

  public void read(CheckpointProvider checkpointProvider, String superKey)
      throws IOException {
    Map<Integer, Checkpoint> thisChkpoint = new TreeMap<Integer, Checkpoint>();
    for (Integer id : idList) {
      byte[] chkpointData = null;
      try {
        chkpointData = checkpointProvider.read(getChkpointKey(superKey, id));
      } catch (Exception e) {
        throw new IOException("Could not read checkpoint ", e);
      }
      Checkpoint checkpoint;
      if (chkpointData != null) {
        checkpoint = new Checkpoint(chkpointData);
      } else {
        checkpoint = new Checkpoint();
      }
      thisChkpoint.put(id, checkpoint);
    }
    setCheckpoint(thisChkpoint);
  }
}
