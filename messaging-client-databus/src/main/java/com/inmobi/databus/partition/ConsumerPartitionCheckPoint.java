package com.inmobi.databus.partition;

import com.inmobi.databus.files.StreamFile;

import java.io.DataInput;
import java.io.IOException;

public class ConsumerPartitionCheckPoint extends PartitionCheckpoint {
  private Integer minId;
  private boolean eofPrevFile;
  private Integer prevMinId;

  public ConsumerPartitionCheckPoint(StreamFile streamFile, long lineNum,
      Integer minId) {
    this(streamFile, lineNum);
    this.minId = minId;
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

  public boolean isEofPrevFile() {
    return eofPrevFile;
  }

  public void setEofPrevFile(boolean eofPrevFile) {
    this.eofPrevFile = eofPrevFile;
  }

  public Integer getPrevMinId() {
    return prevMinId;
  }

  public void setPrevMinId(Integer prevMinId) {
    this.prevMinId = prevMinId;
  }
}
