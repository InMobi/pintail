package com.inmobi.messaging.consumer.databus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class PartitionCheckpoint implements Writable {
  private String fileName;
  private long lineNum;

  PartitionCheckpoint(String fileName, long lineNum) {
    this.fileName = fileName;
    this.lineNum = lineNum;
  }

  PartitionCheckpoint(DataInput in) throws IOException {
    readFields(in);
  }

  public String getFileName() {
    return fileName;
  }

  public long getLineNum() {
    return lineNum;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fileName = in.readUTF();
    lineNum = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(fileName);
    out.writeLong(lineNum);
  }

  public String toString() {
    return fileName + "-" + lineNum;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
    result = prime * result + (int) (lineNum ^ (lineNum >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PartitionCheckpoint other = (PartitionCheckpoint) obj;
    if (fileName == null) {
      if (other.fileName != null)
        return false;
    } else if (!fileName.equals(other.fileName))
      return false;
    if (lineNum != other.lineNum)
      return false;
    return true;
  }

}
