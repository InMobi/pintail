package com.inmobi.messaging.consumer.databus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PartitionCheckpoint implements Writable {
  private String fileName;
  private long offset;

  PartitionCheckpoint(String fileName, long offset) {
    this.fileName = fileName;
    this.offset = offset;
  }

  PartitionCheckpoint(DataInput in) throws IOException {
    readFields(in);
  }

  public String getFileName() {
    return fileName;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fileName = in.readUTF();
    offset = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(fileName);
    out.writeLong(offset);
  }

  public String toString() {
    return fileName + "-" + offset;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
    result = prime * result + (int) (offset ^ (offset >>> 32));
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
    if (offset != other.offset)
      return false;
    return true;
  }

}
