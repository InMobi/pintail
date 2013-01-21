package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.inmobi.databus.partition.ConsumerPartitionCheckPoint;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;

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

  /**
   * This method is used for updating the checkpoint list. This method is used 
   * by only CheckpointUtil class. This is no longer useful after removing the 
   * CheckpointUtil utility. So we can remove it after removing the 
   * migrate-checkpoint utility.
   */
  public void setForCheckpointUtil(PartitionId pid, MessageCheckpoint msgCkp) {
    PartitionCheckpointList pckList = (PartitionCheckpointList) msgCkp;
    for (Map.Entry<Integer, PartitionCheckpoint> entry : pckList.
        getCheckpoints().entrySet()) {
      Checkpoint cp = chkpoints.get(entry.getKey());
      if (cp == null) {
        Map<PartitionId, PartitionCheckpoint> partitionsChkPoints = 
            new HashMap<PartitionId, PartitionCheckpoint>();
        cp = new Checkpoint(partitionsChkPoints);
      }
      cp.set(pid, entry.getValue());
      chkpoints.put(entry.getKey(), cp);
    }
  }

  @Override
  public void set(PartitionId pid, MessageCheckpoint msgCkp) {
    ConsumerPartitionCheckPoint checkPoint = (ConsumerPartitionCheckPoint) msgCkp;
    Checkpoint cp = chkpoints.get(checkPoint.getMinId());
    HashMap<PartitionId,PartitionCheckpoint> map = null;
    if(cp == null) {
      map = new HashMap<PartitionId, PartitionCheckpoint>();
      map.put(pid,new PartitionCheckpoint(checkPoint.getStreamFile(), 
          checkPoint.getLineNum()));
      cp = new Checkpoint(map);
    } else {
      cp.set(pid,new PartitionCheckpoint(checkPoint.getStreamFile(), 
          checkPoint.getLineNum()));
    }
    chkpoints.put(checkPoint.getMinId(),cp);
    //If the EOF is reached for previous file, update its checkpoint to point -1
    if(checkPoint.isEofPrevFile()) {
      Checkpoint prevCp = chkpoints.get(checkPoint.getPrevMinId());
      //If we don't have checkpoint for previous minute which should never 
      //happen, we ignore the setting of checkpoint
      if(prevCp != null) {
        Map<PartitionId,PartitionCheckpoint> prevPartitionCheckPoint = prevCp.
            getPartitionsCheckpoint();
        PartitionCheckpoint pCkP = prevPartitionCheckPoint.get(pid);
        PartitionCheckpoint newPCkp = new PartitionCheckpoint(
            pCkP.getStreamFile(),-1);
        prevPartitionCheckPoint.put(pid,newPCkp);
        chkpoints.put(checkPoint.getPrevMinId(),new Checkpoint(
            prevPartitionCheckPoint));
      }
    }
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
    for (Map.Entry<Integer, Checkpoint> entry : chkpoints.entrySet()) {
      checkpointProvider.checkpoint(getChkpointKey(superKey, entry.getKey()),
          entry.getValue().toBytes());
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
      byte[] chkpointData = checkpointProvider.read(getChkpointKey(superKey, id));
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
