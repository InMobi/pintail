package com.inmobi.databus.partition;

import com.inmobi.databus.files.StreamFile;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConsumerPartitionCheckPoint extends PartitionCheckpoint {
  private Integer minId;
  private Map<Integer, PartitionCheckpoint> deltaCheckpoint;
  private Map<Integer, PartitionCheckpoint> currentMsgCheckpoint;

  public ConsumerPartitionCheckPoint(StreamFile streamFile, long lineNum,
      Integer minId) {
    this(streamFile, lineNum);
    this.minId = minId;
    currentMsgCheckpoint = new HashMap<Integer, PartitionCheckpoint>();
    deltaCheckpoint = new HashMap<Integer, PartitionCheckpoint>();
  }

  public ConsumerPartitionCheckPoint(StreamFile streamFile, long lineNum) {
    super(streamFile, lineNum);
  }

  public ConsumerPartitionCheckPoint(DataInput in) throws IOException {
    super(in);
    //This method is stub for as we are extending the Parent, since this class
    //is not serialized we don't need to
    //worry about constructor being called.
  }


  @Override
  public String toString() {
    StringBuilder strB = new StringBuilder(super.toString());
    strB.append("-").append(minId);
    return strB.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hashCode = super.hashCode();
    hashCode = prime * hashCode +  (minId);
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    boolean equals = super.equals(obj);
    if (equals) {
      if (obj instanceof ConsumerPartitionCheckPoint) {
        ConsumerPartitionCheckPoint other = (ConsumerPartitionCheckPoint) obj;
        return this.minId.equals(other.minId);
      } else {
        return false;
      }
    } else {
      return equals;
    }

  }

  public Integer getMinId() {
    return this.minId;
  }

  public Map<Integer, PartitionCheckpoint> getDeltaCheckpoint() {
    return deltaCheckpoint;
  }

  public void setDeltaCheckpoint(
      Map<Integer, PartitionCheckpoint> deltaCheckpoint) {
    this.deltaCheckpoint = deltaCheckpoint;
  }

  public Map<Integer, PartitionCheckpoint> getCurrentMsgChkpoint() {
    PartitionCheckpoint msgPck = new PartitionCheckpoint(getStreamFile(),
        getLineNum());
    currentMsgCheckpoint.put(getMinId(), msgPck);
    return currentMsgCheckpoint;
  }
}
