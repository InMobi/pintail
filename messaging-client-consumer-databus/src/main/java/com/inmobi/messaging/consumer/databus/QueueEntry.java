package com.inmobi.messaging.consumer.databus;

import com.inmobi.messaging.Message;

public class QueueEntry {

  final Message message;
  final PartitionCheckpoint partitionChkpoint;

  QueueEntry(Message msg, PartitionCheckpoint partitionChkpoint) {
    this.message = msg;
    this.partitionChkpoint = partitionChkpoint;
  }
}
