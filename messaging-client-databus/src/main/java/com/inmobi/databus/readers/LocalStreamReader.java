package com.inmobi.databus.readers;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.partition.PartitionId;

public class LocalStreamReader extends DatabusStreamWaitingReader {

  public LocalStreamReader(PartitionId partitionId, 
      Cluster cluster, String streamName,
      long waitTimeForFileCreate, boolean noNewFiles) throws IOException {
    super(partitionId, cluster, streamName, waitTimeForFileCreate);
    this.noNewFiles = noNewFiles;
  }

  @Override
  protected Path getStreamDir(Cluster cluster, String streamName) {
    return new Path(cluster.getLocalFinalDestDirRoot(), streamName);
  }
}
