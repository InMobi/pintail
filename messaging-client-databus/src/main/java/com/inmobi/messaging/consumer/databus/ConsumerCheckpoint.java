package com.inmobi.messaging.consumer.databus;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
