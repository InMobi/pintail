package com.inmobi.messaging;

import java.nio.ByteBuffer;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 *  Partitioned Message class holding the data and partition identifier of the source and sequence number of record in
 *  the partition. The Sequence number is expected to be a monotonically increasing value.
 *
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class PartitionedMessage extends Message {

  private final Partition partitionIdentifier;
  private final RecordSequence recordSequence;

  public PartitionedMessage(ByteBuffer byteBuffer, Partition partition, RecordSequence recordSequence) {
    super(byteBuffer);
    this.partitionIdentifier = partition;
    this.recordSequence = recordSequence;
  }

  public PartitionedMessage(byte[] data, Partition partition, RecordSequence recordSequence) {
    super(data);
    this.partitionIdentifier = partition;
    this.recordSequence = recordSequence;
  }

  public PartitionedMessage(ByteBuffer byteBuffer, Callback callback, Partition partition, RecordSequence recordSequence) {
    super(byteBuffer, callback);
    this.partitionIdentifier = partition;
    this.recordSequence = recordSequence;
  }

  public PartitionedMessage(byte[] data, Callback callback, Partition partition, RecordSequence recordSequence) {
    super(data, callback);
    this.partitionIdentifier = partition;
    this.recordSequence = recordSequence;
  }
}
