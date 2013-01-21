package com.inmobi.messaging.consumer.util;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.CheckpointProvider;

import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.ClientConfig;

import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.databus.AbstractMessagingDatabusConsumer;
import com.inmobi.messaging.consumer.databus.Checkpoint;
import com.inmobi.messaging.consumer.databus.CheckpointList;
import com.inmobi.messaging.consumer.databus.DatabusConsumer;
import com.inmobi.messaging.consumer.databus.DatabusConsumerConfig;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.hadoop.HadoopConsumer;
import com.inmobi.messaging.consumer.hadoop.HadoopConsumerConfig;

/**
 * This Utility takes consumer configuration as input and creates a list of new
 * checkpoints. This utility is applicable for only where the stream types of 
 * consumers  are “LOCAL” or “MERGED”. This utility reads the consumer 
 * configuration file and read the checkpoint from “consumername_topicname.ck” 
 * file and creates a list of 60 new checkpoint files with the names of 
 * “consumername_topicname_id.ck”.Ex: if old checkpoint file is 
 * l1_benchmark_local.ck then new checkpoint files will be 
 * l1_benchmark_local_0.ck,l1_benchmark_local_1.ck,....,l1_benchmark_local_59.ck.
 */

public class CheckpointUtil implements DatabusConsumerConfig {

  private static final Log LOG = LogFactory.getLog(CheckpointUtil.class);

  public static void prepareCheckpointList(String superKey,
      CheckpointProvider provider, Set<Integer> idList,
      Map<PartitionId, Path> streamDirs,
      CheckpointList checkpointList) 
          throws IOException {
    Checkpoint oldCheckpoint = null;
    byte[] chkpointData = provider.read(superKey);
    if (chkpointData != null) {
      oldCheckpoint = new Checkpoint(chkpointData);
    }

    if (oldCheckpoint == null) {
      LOG.info("Old checkpoint is not available nothing to prepare");
      return;
    }

    Map<PartitionId, PartitionCheckpoint> partitionCheckpoints = 
        oldCheckpoint.getPartitionsCheckpoint();
    for (Map.Entry<PartitionId, PartitionCheckpoint> entry : 
      partitionCheckpoints.entrySet()) {
      PartitionId pid = entry.getKey();
      Path streamDir = streamDirs.get(pid);
      HadoopStreamFile streamFile = (HadoopStreamFile)entry.getValue().
          getStreamFile();
      Map<Integer, PartitionCheckpoint> thisChkpoint =
          new TreeMap<Integer, PartitionCheckpoint>();
      Calendar chkCal = Calendar.getInstance();
      Date checkpointDate = DatabusStreamWaitingReader.getDateFromCheckpointPath(
          streamFile.getCheckpointPath());
      chkCal.setTime(checkpointDate);
      int checkpointMin = chkCal.get(Calendar.MINUTE);
      Calendar chkPrevHrCal = Calendar.getInstance();
      chkPrevHrCal.setTime(checkpointDate);
      /*
       * if the old checkpoint is on 2nd hour 05th minute, minutes [06-59], the 
       * checkpoint will be having the last file of the previous hour directory. 
       * All these checkpoints should constructed and given to the consumer to 
       * start.
       */
      
      chkPrevHrCal.add(Calendar.MINUTE, 1);
      chkPrevHrCal.add(Calendar.HOUR, -1);
      while (chkPrevHrCal.before(chkCal)) {
        if (chkPrevHrCal.get(Calendar.MINUTE) == 0) {
          break;
        }
        Path minDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
            chkPrevHrCal.getTime());
        FileStatus lastElement = getLast(minDir);
        if (lastElement != null) {
          thisChkpoint.put(chkPrevHrCal.get(Calendar.MINUTE),
              new PartitionCheckpoint(new HadoopStreamFile(
                  lastElement.getPath().getParent(),
                  lastElement.getPath().getName(),
                  lastElement.getModificationTime()),
                  -1));
        }
        chkPrevHrCal.add(Calendar.MINUTE, 1);
      }
      
      /*
       * if the old checkpoint is on 2nd hour 05th minute, Then for the minutes 
       * of [00 - 04], the checkpoint should be constructed as 2nd hour 
       * minute directory with last file in the directory. 05th minute will 
       * have same checkpoint as the one read.
       */
      Calendar chkHrCal = Calendar.getInstance();
      chkHrCal.setTime(checkpointDate);
      chkHrCal.set(Calendar.MINUTE, 0);
      while (chkHrCal.before(chkCal)) {
        Path minDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
            chkHrCal.getTime());
        FileStatus lastElement = getLast(minDir);
        if (lastElement != null) {
          thisChkpoint.put(chkHrCal.get(Calendar.MINUTE),
              new PartitionCheckpoint(new HadoopStreamFile(
                  lastElement.getPath().getParent(),
                  lastElement.getPath().getName(),
                  lastElement.getModificationTime()),
                  -1));
        }
        chkHrCal.add(Calendar.MINUTE, 1);
      }
      /*
       * if the old checkpoint is on 2nd hour 05th minute, 05th minute will 
       * have same checkpoint as the one read.
       */
      thisChkpoint.put(checkpointMin, entry.getValue());
      checkpointList.setForCheckpointUtil(entry.getKey(), new 
          PartitionCheckpointList(thisChkpoint));  
    }
  }


  private static FileStatus getLast(final Path minDir)
      throws IOException {

    FileMap<HadoopStreamFile> fmap = new FileMap<HadoopStreamFile>() {
      @Override
      protected TreeMap<HadoopStreamFile, FileStatus> createFilesMap() {
        return new TreeMap<HadoopStreamFile, FileStatus>();
      }

      @Override
      protected HadoopStreamFile getStreamFile(String fileName) {
        throw new RuntimeException("Not implemented");
      }

      @Override
      protected HadoopStreamFile getStreamFile(FileStatus file) {
        return HadoopStreamFile.create(file);
      }

      @Override
      protected PathFilter createPathFilter() {
        return new PathFilter() {
          @Override
          public boolean accept(Path path) {
            if (path.getName().startsWith("_")) {
              return false;
            }
            return true;
          }          
        };
      }

      @Override
      protected void buildList() throws IOException {
        FileSystem fs = minDir.getFileSystem(new Configuration());
        doRecursiveListing(fs, minDir, pathFilter, this);
      }
    };

    fmap.build();

    return fmap.getLastFile();
  }

  private static void doRecursiveListing(FileSystem fs, Path dir,
      PathFilter pathFilter,
      FileMap<HadoopStreamFile> fmap) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(dir, pathFilter);
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("No files in directory:" + dir);
    } else {
      for (FileStatus file : fileStatuses) {
        if (file.isDir()) {
          doRecursiveListing(fs, file.getPath(), pathFilter, fmap);
        } else {
          fmap.addPath(file);
        }
      }
    }
  }

  public static void run(String args[]) throws Exception {
    String confFile = args[0];
    ClientConfig config;
    CheckpointProvider checkpointProvider;
    Set<Integer> idList = new TreeSet<Integer>();
    config = ClientConfig.load(confFile);
    String chkpointProviderClassName = config.getString(
        chkProviderConfig, DEFAULT_CHK_PROVIDER);
    String databusCheckpointDir = config.getString(checkpointDirConfig, 
        DEFAULT_CHECKPOINT_DIR);
    checkpointProvider = AbstractMessagingDatabusConsumer.createCheckpointProvider(
        chkpointProviderClassName, databusCheckpointDir);

    for (int i = 0; i < 60; i++) {
      idList.add(i);
    }
    String topicName = config.getString(MessageConsumerFactory.TOPIC_NAME_KEY);
    String consumerName = config.getString(MessageConsumerFactory.
        CONSUMER_NAME_KEY);
    String superKey = consumerName + "_" + topicName;
    String rootDirConfig;
    Map<PartitionId, Path> streamDirs = new HashMap<PartitionId, Path>();
    String[] rootDirs;
    if (config.getString(databusRootDirsConfig) != null) {
      String type = config.getString(databusStreamType, StreamType.COLLECTOR.
          name());
      StreamType streamType = StreamType.valueOf(type);
      if (streamType.equals(StreamType.COLLECTOR)) {
        LOG.info("No migration required");
        return;
      }
      rootDirConfig = config.getString(databusRootDirsConfig);
      rootDirs = rootDirConfig.split(",");
      int i = 0;
      for (String rootDir : rootDirs) {
        Path streamDir = DatabusUtil.getStreamDir(streamType, new Path(rootDir),
            topicName);
        streamDirs.put(new PartitionId(DatabusConsumer.clusterNamePrefix + i, 
            null), streamDir);
        i++;
      }
    } else if (config.getString(HadoopConsumerConfig.rootDirsConfig) != null) {
      rootDirConfig = config.getString(HadoopConsumerConfig.rootDirsConfig);
      rootDirs = rootDirConfig.split(",");
      int i = 0;
      for (String rootDir : rootDirs) {
        Path streamDir = new Path(rootDir);
        streamDirs.put(new PartitionId(HadoopConsumer.clusterNamePrefix + i, 
            null), streamDir);
        i++;
      }
    }
    CheckpointList checkpointList = new CheckpointList(idList);
    CheckpointUtil.prepareCheckpointList(superKey, checkpointProvider, idList,
        streamDirs, checkpointList);
    checkpointList.write(checkpointProvider, superKey);
    LOG.info("migrated checkpoint " + checkpointList);
    checkpointList.read(checkpointProvider, superKey); 
  }

  public static void main(String [] args) throws Exception {
    if (args.length == 1) {
      run(args);
    } else {
      System.out.println("incorrect number of arguments. provide one " +
          "argument : " + "path to configuration file ");
      System.exit(1);
    }
  }
}


