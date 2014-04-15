package com.inmobi.messaging.consumer.databus;

import java.io.IOException;

import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.checkpoint.CheckpointProvider;

public interface ConsumerCheckpoint {
  public void set(PartitionId pid, MessageCheckpoint pckList);

  public void read(CheckpointProvider checkpointProvider, String key)
      throws IOException;

  public void write(CheckpointProvider checkpointProvider, String key)
      throws IOException;

  public void clear();
}
