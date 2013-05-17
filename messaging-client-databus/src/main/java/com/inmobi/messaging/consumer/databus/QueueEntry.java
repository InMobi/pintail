package com.inmobi.messaging.consumer.databus;

import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.MessageBase;

public class QueueEntry {

  private final MessageBase message;
  private final PartitionId partitionId;
  private final MessageCheckpoint msgCheckpoint;

  public QueueEntry(MessageBase msg, PartitionId partitionId,
      MessageCheckpoint msgCheckpoint) {
    this.message = msg;
    this.partitionId = partitionId;
    this.msgCheckpoint = msgCheckpoint;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public MessageCheckpoint getMessageChkpoint() {
    return msgCheckpoint;
  }

  public MessageBase getMessage() {
    return message;
  }
}
