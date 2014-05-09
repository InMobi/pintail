package com.inmobi.messaging.consumer.hadoop;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2012 - 2014 InMobi
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
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.databus.AbstractMessagingDatabusConsumer;
import com.inmobi.messaging.consumer.databus.CheckpointList;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class HadoopConsumer extends AbstractMessagingDatabusConsumer
    implements HadoopConsumerConfig {

  private Path[] rootDirs;
  private FileSystem[] fileSystems;
  private String inputFormatClassName;
  public static String clusterNamePrefix = "hadoopcluster";

  protected void initializeConfig(ClientConfig config) throws IOException {
    super.initializeConfig(config);

    String rootDirStr = config.getString(rootDirsConfig);
    String[] rootDirStrs;
    if (rootDirStr == null) {
      throw new IllegalArgumentException("Missing root dir configuration: "
          + rootDirsConfig);
    } else {
      rootDirStrs = rootDirStr.split(",");
    }
    rootDirs = new Path[rootDirStrs.length];
    clusterNames = new String[rootDirStrs.length];
    fileSystems = new FileSystem[rootDirStrs.length];
    for (int i = 0; i < rootDirStrs.length; i++) {
      LOG.debug("Looking at rootDirStr:" + rootDirStrs[i]);
      rootDirs[i] = new Path(rootDirStrs[i]);
      fileSystems[i] = rootDirs[i].getFileSystem(conf);
      clusterNames[i] = clusterNamePrefix + i;
    }

    parseClusterNamesAndMigrateCheckpoint(config, rootDirStrs);

    inputFormatClassName = config.getString(inputFormatClassNameConfig,
        DEFAULT_INPUT_FORMAT_CLASSNAME);

  }

  /**
   * Creates one partition reader for one rootdir
   */
  protected void createPartitionReaders() throws IOException {
    for (int i = 0; i < clusterNames.length; i++) {
      String clusterName = clusterNames[i];
      String fsUri = fileSystems[i].getUri().toString();
      LOG.debug("Creating partition reader for cluster:" + clusterName);

      // create partition id
      PartitionId id = new PartitionId(clusterName, null);

      // Get the partition checkpoint list from consumer checkpoint
      PartitionCheckpointList partitionCheckpointList = 
          ((CheckpointList) currentCheckpoint).preaprePartitionCheckPointList(id);

      Date partitionTimestamp = getPartitionTimestamp(id,
          partitionCheckpointList);
      PartitionReaderStatsExposer clusterMetrics =
          new PartitionReaderStatsExposer(topicName, consumerName, id.toString(),
              consumerNumber, fsUri);
      addStatsExposer(clusterMetrics);
      PartitionReader reader = new PartitionReader(id,
          partitionCheckpointList, fileSystems[i], buffer, rootDirs[i],
          conf, inputFormatClassName, partitionTimestamp,
          waitTimeForFileCreate, false, clusterMetrics, partitionMinList,
          stopTime);
      LOG.debug("Created partition " + id);
      readers.put(id, reader);
      messageConsumedMap.put(id, false);
    }
  }

  Path[] getRootDirs() {
    return rootDirs;
  }

  @Override
  protected void createCheckpoint() {
    currentCheckpoint = new CheckpointList(partitionMinList);
  }

}
