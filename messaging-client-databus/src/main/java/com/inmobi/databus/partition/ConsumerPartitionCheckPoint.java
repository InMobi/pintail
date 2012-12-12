package com.inmobi.databus.partition;

import com.inmobi.databus.files.StreamFile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConsumerPartitionCheckPoint extends PartitionCheckpoint {
  private Integer minId;

  public ConsumerPartitionCheckPoint(StreamFile streamFile, long lineNum, Integer minId) {
    this(streamFile, lineNum);
    this.minId = minId;
  }

  public ConsumerPartitionCheckPoint(StreamFile streamFile, long lineNum) {
    super(streamFile, lineNum);
  }

  public ConsumerPartitionCheckPoint(DataInput in) throws IOException {
    super(in);
    this.minId = in.readInt();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.minId = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(minId);
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
    hashCode = prime * hashCode +  (minId ^ (minId >>> 32));
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    boolean equals = super.equals(obj);
    if (equals) {
      if (obj instanceof ConsumerPartitionCheckPoint) {
        ConsumerPartitionCheckPoint other = (ConsumerPartitionCheckPoint) obj;
        if (this.minId == other.minId) {
          return true;
        } else {
          return false;
        }
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
}
