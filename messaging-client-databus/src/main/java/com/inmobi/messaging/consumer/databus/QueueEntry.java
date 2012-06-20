package com.inmobi.messaging.consumer.databus;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.Message;

public class QueueEntry {

  private final Message message;
  private final PartitionId partitionId;
  private final PartitionCheckpoint partitionChkpoint;

  public QueueEntry(Message msg, PartitionId partitionId,
      PartitionCheckpoint partitionChkpoint) {
    this.message = msg;
    this.partitionId = partitionId;
    this.partitionChkpoint = partitionChkpoint;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public PartitionCheckpoint getPartitionChkpoint() {
    return partitionChkpoint;
  }

  public Message getMessage() {
    return message;
  }
}
