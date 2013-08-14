package com.inmobi.databus.partition;

import com.inmobi.databus.files.StreamFile;
import com.inmobi.messaging.consumer.databus.MessageCheckpoint;

import java.util.HashMap;
import java.util.Map;

public class DeltaPartitionCheckPoint implements MessageCheckpoint {
  private final Map<Integer, PartitionCheckpoint> deltaCheckpoint =
      new HashMap<Integer, PartitionCheckpoint>();

  public DeltaPartitionCheckPoint(StreamFile streamFile, long lineNum,
      Integer minId, Map<Integer, PartitionCheckpoint> deltaCheckpoint) {
    this.deltaCheckpoint.putAll(deltaCheckpoint);
    this.deltaCheckpoint.put(minId,
        new PartitionCheckpoint(streamFile, lineNum));
  }

  public DeltaPartitionCheckPoint(
      Map<Integer, PartitionCheckpoint> deltaCheckpoint) {
    this.deltaCheckpoint.putAll(deltaCheckpoint);
  }

  @Override
  public String toString() {
    return this.deltaCheckpoint.toString();
  }

  public Map<Integer, PartitionCheckpoint> getDeltaCheckpoint() {
    return deltaCheckpoint;
  }

  @Override
  public boolean isNULL() {
    return false;
  }
}
