package com.inmobi.databus.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PartitionId implements Writable {

  private String cluster;
  private String collector;

  public PartitionId(String cluster, String collector) {
    this.cluster = cluster;
    this.collector = collector;
  }

  public PartitionId(DataInput in) throws IOException {
    readFields(in);
  }

  public String getCluster() {
    return cluster;
  }

  public String getCollector() {
    return collector;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    cluster = in.readUTF();
    boolean notNull = in.readBoolean();
    if (notNull) {
      collector = in.readUTF();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(cluster);
    boolean notNull = collector != null;
    out.writeBoolean(notNull);
    if (notNull) {
      out.writeUTF(collector);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
    result = prime * result + ((collector == null) ? 0 : collector.hashCode());
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
    PartitionId other = (PartitionId) obj;
    if (cluster == null) {
      if (other.cluster != null) {
        return false;
      }
    } else if (!cluster.equals(other.cluster)) {
      return false;
    }
    if (collector == null) {
      if (other.collector != null) {
        return false;
      }
    } else if (!collector.equals(other.collector)) {
      return false;
    }
    return true;
  }

  public String toString() {
    if (collector != null) {
      return cluster + "-" + collector;
    } else {
      return cluster;
    }
  }

}
