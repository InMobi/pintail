package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.MessageCheckpoint;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class ClusterReader extends AbstractPartitionStreamReader {

  private static final Log LOG = LogFactory.getLog(PartitionReader.class);

  private final PartitionCheckpointList partitionCheckpointList;
  private final Date startTime;
  private final Path streamDir;
  private final Date stopDate;
  private final boolean isDatabusData;

  ClusterReader(PartitionId partitionId,
      PartitionCheckpointList partitionCheckpointList, FileSystem fs,
      Path streamDir, Configuration conf, String inputFormatClass,
      Date startTime, long waitTimeForFileCreate, boolean isDatabusData,
      PartitionReaderStatsExposer metrics, boolean noNewFiles,
      Set<Integer> partitionMinList, Date stopDate)
          throws IOException {
    this.startTime = startTime;
    this.stopDate = stopDate;
    this.streamDir = streamDir;
    //create an empty partition checkpoint list if the start time is not null
    if (startTime != null) {
      Map<Integer, PartitionCheckpoint> startPckList;
      startPckList = new HashMap<Integer, PartitionCheckpoint>();
      partitionCheckpointList = new PartitionCheckpointList(startPckList);
    }
    this.partitionCheckpointList = partitionCheckpointList;
    this.isDatabusData = isDatabusData;

    reader = new DatabusStreamWaitingReader(partitionId, fs, streamDir,
        inputFormatClass, conf, waitTimeForFileCreate, metrics, noNewFiles,
        partitionMinList, partitionCheckpointList);
  }

  /*
   * this method is used to find the partition checkpoint which has least 
   * time stamp.
   * So that reader starts build listing from this partition checkpoint  
   * time stamp).
   */
  public static PartitionCheckpoint findLeastPartitionCheckPointTime(
      PartitionCheckpointList partitionCheckpointList) {
    PartitionCheckpoint partitioncheckpoint = null;
    Map<Integer, PartitionCheckpoint> listOfCheckpoints = 
        partitionCheckpointList.getCheckpoints();

    if (listOfCheckpoints != null) {
      Collection<PartitionCheckpoint> listofPartitionCheckpoints = 
          listOfCheckpoints.values();
      Iterator<PartitionCheckpoint> it = listofPartitionCheckpoints.iterator();
      Date timeStamp = null;
      if (it.hasNext()) {
        partitioncheckpoint = it.next();
        if (partitioncheckpoint != null) {
          timeStamp = DatabusStreamWaitingReader.getDateFromCheckpointPath(
              partitioncheckpoint.getFileName());
        }
      }
      while (it.hasNext()) {
        PartitionCheckpoint tmpPartitionCheckpoint = it.next();
        if (tmpPartitionCheckpoint != null) {
          Date date = DatabusStreamWaitingReader.getDateFromCheckpointPath(
              tmpPartitionCheckpoint.getFileName());
          if (timeStamp.compareTo(date) > 0) {
            partitioncheckpoint = tmpPartitionCheckpoint;
            timeStamp = date;
          }
        }
      } 
    }
    return partitioncheckpoint;
  }

  public void initializeCurrentFile() throws IOException, InterruptedException {
    LOG.info("Initializing partition reader's current file");
    PartitionCheckpoint partitionCheckpoint = null;
    if (partitionCheckpointList != null) {
      partitionCheckpoint = findLeastPartitionCheckPointTime(
          partitionCheckpointList);
    } 

    if (startTime != null) {
      ((DatabusStreamWaitingReader)reader).build(startTime);
      if (!reader.initializeCurrentFile(startTime)) {
        LOG.debug("Did not find the file associated with timestamp");
        reader.startFromTimestmp(startTime);
      }
    } else if (partitionCheckpoint != null) {
      ((DatabusStreamWaitingReader)reader).build(
          DatabusStreamWaitingReader.getBuildTimestamp(streamDir,
              partitionCheckpoint));
      // check whether the given stop date is beyond the checkpoint
      isStopDateBeyondCheckpoint(partitionCheckpoint);
      if (!reader.isEmpty()) {
        // if the partition checkpoint is completed checkpoint
        //(i.e. line number is -1) then it has to start from the next checkpoint.
        if (partitionCheckpoint.getLineNum() == -1) {
          ((DatabusStreamWaitingReader)reader).initFromNextCheckPoint(); 
        }
        else if (!reader.initializeCurrentFile(partitionCheckpoint)) {
          throw new IllegalArgumentException("Checkpoint file does not exist");
        }
      } else {
        reader.startFromBegining();
      }
    } else {
      LOG.info("Would never reach here");
    }
    LOG.info("Intialized currentFile:" + reader.getCurrentFile() +
        " currentLineNum:" + reader.getCurrentLineNum());
  }

  private void isStopDateBeyondCheckpoint(
      PartitionCheckpoint partitionCheckpoint) throws IOException {
    String currentFile = partitionCheckpoint.getFileName();
    Date currentTimeStamp = DatabusStreamWaitingReader.
        getDateFromCheckpointPath(currentFile);
    if (stopDate != null && stopDate.before(currentTimeStamp)) {
      throw new IllegalArgumentException("Invalid stopDate is provided " +
          "i.e. stop date is beyond the checkpoint ");
    }
  }

  @Override
  public MessageCheckpoint getMessageCheckpoint() {
    DatabusStreamWaitingReader dataWaitingReader = 
        (DatabusStreamWaitingReader) reader;
    boolean movedToNext = dataWaitingReader.isMovedToNext();
    ConsumerPartitionCheckPoint consumerPartitionCheckPoint =
        new ConsumerPartitionCheckPoint(dataWaitingReader.getCurrentStreamFile(),
            dataWaitingReader.getCurrentLineNum(),dataWaitingReader.
            getCurrentMin());
    //Check after getting message checkpoint, if Partition Reader has moved to 
    //next file. If yes, then set the flags in the checkpoint and reset reader 
    //flags.
    if(movedToNext) {
      consumerPartitionCheckPoint.setEofPrevFile(movedToNext);
      consumerPartitionCheckPoint.setPrevMinId(dataWaitingReader.getPrevMin());
      dataWaitingReader.resetMoveToNextFlags();
    }
    return consumerPartitionCheckPoint;
  }
}