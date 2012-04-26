package com.inmobi.messaging.consumer.databus;

import com.inmobi.messaging.Message;

public class QueueEntry {

  final Message message;
  final PartitionId partitionId;
  final PartitionCheckpoint partitionChkpoint;

  QueueEntry(Message msg, PartitionId partitionId,
      PartitionCheckpoint partitionChkpoint) {
    this.message = msg;
    this.partitionId = partitionId;
    this.partitionChkpoint = partitionChkpoint;
  }
}
