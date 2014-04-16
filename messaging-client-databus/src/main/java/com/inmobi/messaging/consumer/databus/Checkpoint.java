package com.inmobi.messaging.consumer.databus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.checkpoint.CheckpointProvider;

/**
 * Checkpoint for the databus stream.
 *
 * It holds checkpoint for all the partitions.
 *
 */
public class Checkpoint implements Writable, ConsumerCheckpoint {

  // map of partitionId to partition
  private final Map<PartitionId, PartitionCheckpoint> partitionsChkPoint =
      new HashMap<PartitionId, PartitionCheckpoint>();

  public Checkpoint() {
  }

  public Checkpoint(byte[] bytes) throws IOException {
    readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public byte[] toBytes() throws IOException {
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bOut);
    write(out);
    return bOut.toByteArray();
  }

  public Map<PartitionId, PartitionCheckpoint> getPartitionsCheckpoint() {
    return partitionsChkPoint;
  }

  public void set(PartitionId partitionId, MessageCheckpoint partCheckpoint) {
    partitionsChkPoint.put(partitionId, (PartitionCheckpoint) partCheckpoint);
  }


  public void remove(PartitionId pid) {
    partitionsChkPoint.remove(pid);
  }

  @Override
  public void read(CheckpointProvider checkpointProvider, String key)
      throws IOException {
    byte[] chkpointData = null;
    try {
      chkpointData = checkpointProvider.read(key);
    } catch (Exception e) {
      throw new IOException("Could not read checkpoint.", e);
    }
    if (chkpointData != null) {
      readFields(new DataInputStream(new ByteArrayInputStream(chkpointData)));
    }
  }

  @Override
  public void write(CheckpointProvider checkpointProvider, String key)
      throws IOException {
    try {
      checkpointProvider.checkpoint(key, this.toBytes());
    } catch (Exception e) {
      throw new IOException("Could not checkpoint. ", e);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      PartitionId pid = new PartitionId(in);
      boolean valueNotNull = in.readBoolean();
      if (valueNotNull) {
        partitionsChkPoint.put(pid, new PartitionCheckpoint(in));
      } else {
        partitionsChkPoint.put(pid, null);
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(partitionsChkPoint.size());
    for (Map.Entry<PartitionId, PartitionCheckpoint> entry : partitionsChkPoint
        .entrySet()) {
      entry.getKey().write(out);
      if (entry.getValue() == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        entry.getValue().write(out);
      }
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((partitionsChkPoint == null) ? 0 : partitionsChkPoint.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Checkpoint other = (Checkpoint) obj;
    if (partitionsChkPoint == null) {
      if (other.partitionsChkPoint != null) {
        return false;
      }
    } else if (!partitionsChkPoint.equals(other.partitionsChkPoint)) {
      return false;
    }
    return true;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    for (Map.Entry<PartitionId, PartitionCheckpoint> entry : partitionsChkPoint
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

  @Override
  public void clear() {
    partitionsChkPoint.clear();
  }

  public void migrateCheckpoint(PartitionCheckpoint pck,
      PartitionId defaultPid, PartitionId newPid) {
    /*
     * Create a checkpoint with new pid and partition checkpoint.
     * Remove an entry of default/old pid from checkpoint as it does not
     * useful anymore
     */
    set(newPid, pck);
    remove(defaultPid);
  }
}
