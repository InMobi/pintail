package com.inmobi.databus.readers;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class DatabusStreamWaitingReader
    extends DatabusStreamReader<HadoopStreamFile> {

  private static final Log LOG = LogFactory.getLog(
      DatabusStreamWaitingReader.class);

  private int currentMin;
  private final Set<Integer> partitionMinList;
  private PartitionCheckpointList partitionCheckpointList;
  private Map<Integer, Date> checkpointTimeStampMap;
  private Map<Integer, PartitionCheckpoint> pChkpoints;
  private Map<Integer, PartitionCheckpoint> deltaCheckpoint;
  private boolean createdDeltaCheckpointForFirstFile;

  public DatabusStreamWaitingReader(PartitionId partitionId, FileSystem fs,
      Path streamDir,  String inputFormatClass, Configuration conf,
      long waitTimeForFileCreate, PartitionReaderStatsExposer metrics,
      boolean noNewFiles, Set<Integer> partitionMinList,
      PartitionCheckpointList partitionCheckpointList, Date stopTime)
          throws IOException {
    super(partitionId, fs, streamDir, inputFormatClass, conf,
        waitTimeForFileCreate, metrics, noNewFiles, stopTime);
    this.partitionCheckpointList = partitionCheckpointList;
    this.partitionMinList = partitionMinList;
    this.stopTime = stopTime;
    currentMin = -1;
    this.checkpointTimeStampMap = new HashMap<Integer, Date>();
    if (partitionCheckpointList != null) {
      pChkpoints = partitionCheckpointList.getCheckpoints();
      prepareTimeStampsOfCheckpoints();
    }
    deltaCheckpoint = new HashMap<Integer, PartitionCheckpoint>();
    createdDeltaCheckpointForFirstFile = false;
  }

  public void prepareTimeStampsOfCheckpoints() {
    PartitionCheckpoint partitionCheckpoint = null;
    for (Integer min : partitionMinList) {
      partitionCheckpoint = pChkpoints.get(min);
      if (partitionCheckpoint != null) {
        Date checkpointedTimestamp = getDateFromCheckpointPath(
            partitionCheckpoint.getFileName());
        checkpointTimeStampMap.put(min, checkpointedTimestamp);
      } else {
        checkpointTimeStampMap.put(min, null);
      }
    }
  }


  /**
   * This method is used to check whether the given minute directory is
   * completely read or not. It takes the current time stamp and the minute
   * on which the reader is currently working. It retrieves the partition checkpoint
   * for that minute if it contains. It compares the current time stamp with
   * the checkpointed time stamp. If current time stamp is before the
   * checkpointed time stamp then that minute directory for the current hour is
   * completely read. If both the time stamps are same then it checks line number.
   * If line num is -1 means all the files in that minute dir are already read.
   */
  public boolean isRead(Date currentTimeStamp, int minute) {
    Date checkpointedTimestamp = checkpointTimeStampMap.get(
        Integer.valueOf(minute));
    if (checkpointedTimestamp == null) {
      return false;
    }
    if (currentTimeStamp.before(checkpointedTimestamp)) {
      return true;
    } else if (currentTimeStamp.equals(checkpointedTimestamp)) {
      PartitionCheckpoint partitionCheckpoint = pChkpoints.get(
          Integer.valueOf(minute));
      if (partitionCheckpoint != null && partitionCheckpoint.getLineNum() == -1)
      {
        return true;
      }
    }
    return false;
  }

  /**
   * It reads from the next checkpoint. It retrieves the first file from the filemap.
   * Get the minute id from the file and see the checkpoint value. If the
   * checkpointed file is not same as current file then it sets the iterator to
   * the checkpointed file if the checkpointed file exists.
   */
  public boolean initFromNextCheckPoint() throws IOException {
    initCurrentFile();
    currentFile = getFirstFileInStream();
    Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        currentFile.getPath().getParent());
    Calendar current = Calendar.getInstance();
    current.setTime(date);
    int currentMinute = current.get(Calendar.MINUTE);
    PartitionCheckpoint partitioncheckpoint = partitionCheckpointList.
        getCheckpoints().get(currentMinute);

    if (partitioncheckpoint != null) {
      Path checkpointedFileName = new Path(streamDir,
          partitioncheckpoint.getFileName());
      if (!(currentFile.getPath()).equals(checkpointedFileName)) {
        if (fsIsPathExists(checkpointedFileName)
            && (partitioncheckpoint.getLineNum() != -1)) {
          currentFile = fsGetFileStatus(checkpointedFileName);
          currentLineNum = partitioncheckpoint.getLineNum();
        } else {
          currentLineNum = 0;
        }
      } else {
        currentLineNum = partitioncheckpoint.getLineNum();
      }
    }
    if (currentFile != null) {
      LOG.debug("CurrentFile:" + getCurrentFile() + " currentLineNum:"
          + currentLineNum);
      setIterator();
    }
    return currentFile != null;
  }

  @Override
  protected void buildListing(FileMap<HadoopStreamFile> fmap,
      PathFilter pathFilter) throws IOException {
    if (!setBuildTimeStamp(pathFilter)) {
      return;
    }
    Calendar current = Calendar.getInstance();
    Date now = current.getTime();
    current.setTime(buildTimestamp);
    while (current.getTime().before(now)) {
      // stop the file listing if stop date is beyond current time.
      if (checkAndSetstopTimeReached(current)) {
        break;
      }
      int min = current.get(Calendar.MINUTE);
      Date currenTimestamp = current.getTime();
      // Move the current minute to next minute
      current.add(Calendar.MINUTE, 1);
      if (partitionMinList.contains(Integer.valueOf(min))
          && !isRead(currenTimestamp, min)) {
        Path dir = getMinuteDirPath(streamDir, currenTimestamp);
        if (fsIsPathExists(dir)) {
          Path nextMinDir = getMinuteDirPath(streamDir, current.getTime());
          if (fsIsPathExists(nextMinDir)) {
            int numFilesInFileMap = fmap.getSize();
            doRecursiveListing(dir, pathFilter, fmap);
            if (numFilesInFileMap != 0 &&
                numFilesInFileMap != fmap.getSize()) {
              break;
            }
          } else {
            LOG.info("Reached end of file listing. Not looking at the last"
                + " minute directory:" + dir);
            break;
          }
        }
      }
    }

    if (getFirstFileInStream() != null && (currentMin == -1)) {
      FileStatus firstFileInStream = getFirstFileInStream();
      currentMin = getMinuteFromFile(firstFileInStream);
    }
  }

  /*
   * check whether reached stopTime and stop the File listing if it reached stopTime
   */
  private boolean checkAndSetstopTimeReached(Calendar current) {
    if (stopTime != null && stopTime.before(current.getTime())) {
      LOG.info("Reached stopTime. Not listing from after the stop date ");
      stopListing();
      return true;
    }
    return false;
  }

  /**
   * This method does the required setup before moving to next file. First it
   * checks whether the both current file and next file belongs to same minute
   * or different minutes. If files exists on across minutes then it has to
   * check the next file is same as checkpointed file. If not same and checkpointed
   * file exists then sets the iterator to the checkpointed file.
   * @return false if it reads from the checkpointed file.
   */
  @Override
  public boolean prepareMoveToNext(FileStatus currentFile, FileStatus nextFile)
      throws IOException {
    Date currentFileTimeStamp = getDateFromStreamDir(streamDir,
        currentFile.getPath().getParent());
    Calendar now = Calendar.getInstance();
    now.setTime(currentFileTimeStamp);
    currentMin = now.get(Calendar.MINUTE);

    Date nextFileTimeStamp = getDateFromStreamDir(streamDir,
        nextFile.getPath().getParent());
    now.setTime(nextFileTimeStamp);

    boolean readFromCheckpoint = false;
    FileStatus fileToRead = nextFile;
    if (currentMin != now.get(Calendar.MINUTE)) {
      setDeltaCheckpoint(getNextMinuteTimeStamp(currentFileTimeStamp),
          nextFileTimeStamp);
      // set the line number as -1 as current file was read fully.
      deltaCheckpoint.put(currentMin,
          new PartitionCheckpoint(getStreamFile(currentFile), -1));
      currentMin = now.get(Calendar.MINUTE);
      PartitionCheckpoint partitionCheckpoint = partitionCheckpointList.
          getCheckpoints().get(currentMin);
      if (partitionCheckpoint != null && partitionCheckpoint.getLineNum() != -1)
      {
        Path checkPointedFileName = new Path(streamDir,
            partitionCheckpoint.getFileName());
        //set iterator to checkpoointed file if there is a checkpoint
        if (!fileToRead.getPath().equals(checkPointedFileName)) {
          if (fsIsPathExists(checkPointedFileName)) {
            fileToRead = fsGetFileStatus(checkPointedFileName);
            currentLineNum = partitionCheckpoint.getLineNum();
          } else {
            currentLineNum = 0;
          }
        } else {
          currentLineNum = partitionCheckpoint.getLineNum();
        }
        readFromCheckpoint = true;
      }
    }
    this.currentFile = fileToRead;
    setIterator();
    return !readFromCheckpoint;
  }

  private Date getNextMinuteTimeStamp(Date currentFileTimeStamp) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(currentFileTimeStamp);
    cal.add(Calendar.MINUTE, 1);
    return cal.getTime();
  }

  /*
   * prepare a delta checkpoint
   */
  private void setDeltaCheckpoint(Date from, Date to) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    while (cal.getTime().before(to)) {
      Integer currentMinute = Integer.valueOf(cal.get(Calendar.MINUTE));
      Date checkpointedTimeStamp = checkpointTimeStampMap.get(currentMinute);
      if (partitionMinList.contains(currentMinute)) {
        // create a checkpoint for that minute only if it does not have
        // checkpoint or checkpoint file time stamp is older than
        // current file time stamp
        if (checkpointedTimeStamp == null
            || checkpointedTimeStamp.before(cal.getTime())) {
          deltaCheckpoint.put(currentMinute,
              new PartitionCheckpoint(getStreamFile(cal.getTime()), -1));
        }
      }
      cal.add(Calendar.MINUTE, 1);
    }
  }

  @Override
  protected HadoopStreamFile getStreamFile(Date timestamp) {
    Path streamDirPath = streamDir;
    return getHadoopStreamFile(streamDirPath, timestamp);
  }

  protected HadoopStreamFile getStreamFile(FileStatus status) {
    return getHadoopStreamFile(status);
  }

  protected void startFromNextHigher(FileStatus file)
      throws IOException, InterruptedException {
    if (!setNextHigherAndOpen(file)) {
      waitForNextFileCreation(file);
    }
  }

  private void waitForNextFileCreation(FileStatus file)
      throws IOException, InterruptedException {
    while (!closed && !setNextHigherAndOpen(file) && !hasReadFully()) {
      LOG.info("Waiting for next file creation");
      waitForFileCreate();
      build();
    }
  }

  @Override
  public Message readLine() throws IOException, InterruptedException {
    Message line = readNextLine();
    if (!createdDeltaCheckpointForFirstFile) {
      setDeltaCheckpoint(buildTimestamp, getDateFromStreamDir(streamDir,
          getCurrentFile()));
      createdDeltaCheckpointForFirstFile = true;
    }
    while (line == null) { // reached end of file
      LOG.info("Read " + getCurrentFile() + " with lines:" + currentLineNum);
      if (closed) {
        LOG.info("Stream closed");
        break;
      }
      if (!nextFile()) { // reached end of file list
        LOG.info("could not find next file. Rebuilding");
        build(getDateFromStreamDir(streamDir, getCurrentFile()));

        if (!nextFile()) { // reached end of stream
          // stop reading if read till stopTime
          if (hasReadFully()) {
            LOG.info("read all files till stop date");
            break;
          }
          LOG.info("Could not find next file");
          startFromNextHigher(currentFile);
          LOG.info("Reading from next higher file " + getCurrentFile());
        } else {
          LOG.info("Reading from " + getCurrentFile() + " after rebuild");
        }
      } else {
        // read line from next file
        LOG.info("Reading from next file " + getCurrentFile());
      }
      line = readNextLine();
    }
    return line;
  }

  @Override
  protected FileMap<HadoopStreamFile> createFileMap() throws IOException {
    return new FileMap<HadoopStreamFile>() {
      @Override
      protected void buildList() throws IOException {
        buildListing(this, pathFilter);
      }

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
    };
  }

  public static Date getBuildTimestamp(Path streamDir,
      PartitionCheckpoint partitionCheckpoint) {
    try {
      return getDateFromCheckpointPath(partitionCheckpoint.getFileName());
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid checkpoint: "
          + partitionCheckpoint.getStreamFile(), e);
    }
  }

  public static HadoopStreamFile getHadoopStreamFile(FileStatus status) {
    return HadoopStreamFile.create(status);
  }

  public static HadoopStreamFile getHadoopStreamFile(Path streamDirPath,
      Date timestamp) {
    return new HadoopStreamFile(getMinuteDirPath(streamDirPath, timestamp),
        null, null);
  }

  public Map<Integer, PartitionCheckpoint> getDeltaCheckpoint() {
    return deltaCheckpoint;
  }

  public int getCurrentMin() {
    return this.currentMin;
  }

  /**
   * @returns Zero  if checkpoint is not present for that minute or
   *                checkpoint file and current file were not same.
   *          Line number from checkpoint
   */
  @Override
  protected long getLineNumberForFirstFile(FileStatus firstFile) {
    int minute = getMinuteFromFile(firstFile);
    PartitionCheckpoint partitionChkPoint = pChkpoints.get(
        Integer.valueOf(minute));
    if (partitionChkPoint != null) {
      Path checkPointedFileName = new Path(streamDir, partitionChkPoint.
          getFileName());
      // check whether current file and checkpoint file are same
      if (checkPointedFileName.equals(firstFile.getPath())) {
        return partitionChkPoint.getLineNum();
      }
    }
    return 0;
  }

  private int getMinuteFromFile(FileStatus firstFile) {
    Date currentTimeStamp = getDateFromStreamDir(streamDir, firstFile.
        getPath().getParent());
    Calendar cal = Calendar.getInstance();
    cal.setTime(currentTimeStamp);
    return cal.get(Calendar.MINUTE);
  }

  public void resetDeltaCheckpoint() {
    deltaCheckpoint.clear();
  }
}
