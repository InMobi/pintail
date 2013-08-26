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
  private final Set<Integer> partitionMinList;
  private final Map<Integer, CheckpointInfo> pChkpoints =
      new HashMap<Integer, CheckpointInfo>();
  private final Map<Integer, PartitionCheckpoint> deltaCheckpoint;
  private boolean createdDeltaCheckpointForFirstFile;
  private int currentMin;

  private static class CheckpointInfo {
    PartitionCheckpoint pck;
    Date timeStamp;
    boolean processed = false;
    boolean readFully = false;
    
    CheckpointInfo(PartitionCheckpoint pck) {
      this.pck = pck;
      if (pck != null) {
        this.timeStamp = getDateFromCheckpointPath(
          ((HadoopStreamFile)pck.getStreamFile()).getCheckpointPath());
        this.readFully = (pck.getLineNum() == -1);
        if (readFully || (pck.getName() == null)) {
          processed = true;
        }
      } else {
        // nothing to process
        processed = true;
      }
    }
  }

  public DatabusStreamWaitingReader(PartitionId partitionId, FileSystem fs,
      Path streamDir,  String inputFormatClass, Configuration conf,
      long waitTimeForFileCreate, PartitionReaderStatsExposer metrics,
      boolean noNewFiles, Set<Integer> partitionMinList,
      PartitionCheckpointList partitionCheckpointList, Date stopTime)
          throws IOException {
    super(partitionId, fs, streamDir, inputFormatClass, conf,
        waitTimeForFileCreate, metrics, noNewFiles, stopTime);
    this.partitionMinList = partitionMinList;
    currentMin = -1;
    for (Integer min : partitionMinList) {
      CheckpointInfo cpi = new CheckpointInfo(
          partitionCheckpointList.getCheckpoints().get(min));
      pChkpoints.put(min, cpi);
    }
    deltaCheckpoint = new HashMap<Integer, PartitionCheckpoint>();
    createdDeltaCheckpointForFirstFile = false;
  }

  public void initializeBuildTimeStamp(Date buildTimestamp)
      throws IOException {
    this.buildTimestamp = buildTimestamp;
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
    Date checkpointedTimestamp = pChkpoints.get(
        Integer.valueOf(minute)).timeStamp;
    if (checkpointedTimestamp == null) {
      return false;
    }
    if (currentTimeStamp.before(checkpointedTimestamp)) {
      return true;
    } else if (currentTimeStamp.equals(checkpointedTimestamp)) {
      return pChkpoints.get(Integer.valueOf(minute)).readFully;
    }
    return false;
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
              // stopping after listing two non empty directories
              LOG.debug("Listing stopped after listing two non empty directories");
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
   * @throws InterruptedException 
   */
  @Override
  public boolean prepareMoveToNext(FileStatus currentFile, FileStatus nextFile)
      throws IOException, InterruptedException {
    Calendar next = Calendar.getInstance();
    Date nextFileTimeStamp = getDateFromStreamDir(streamDir,
        nextFile.getPath().getParent());
    next.setTime(nextFileTimeStamp);

    boolean readFromCheckpoint = false;
    if (currentMin != next.get(Calendar.MINUTE)) {
      if (currentFile != null) {
        Date currentFileTimeStamp = getDateFromStreamDir(streamDir, currentFile
            .getPath().getParent());
        setDeltaCheckpoint(getNextMinuteTimeStamp(currentFileTimeStamp),
            nextFileTimeStamp);
        // set the line number as -1 as current file was read fully.
        deltaCheckpoint.put(currentMin, new PartitionCheckpoint(
            getStreamFile(currentFile), -1));
      }
      // move to next file
      currentMin = next.get(Calendar.MINUTE);
      readFromCheckpoint = moveToCheckpoint(nextFile);
    } else {
      this.currentFile = nextFile;
    }
    setIterator();
    return !readFromCheckpoint;
  }

  private boolean moveToCheckpoint(FileStatus fileToRead)
      throws IOException, InterruptedException {
    boolean ret = false;
    CheckpointInfo cpi = pChkpoints.get(currentMin);
    if (!cpi.processed) {
      cpi.processed = true;
      PartitionCheckpoint partitionCheckpoint = cpi.pck;
      if (((HadoopStreamFile) partitionCheckpoint.getStreamFile()).getFileName()
          != null) {
        Path checkPointedFileName = new Path(streamDir,
            partitionCheckpoint.getFileName());
        //set iterator to checkpointed file if there is a checkpoint
        if (!fileToRead.getPath().equals(checkPointedFileName)) {
          if (fsIsPathExists(checkPointedFileName)) {
            fileToRead = fsGetFileStatus(checkPointedFileName);
            currentLineNum = partitionCheckpoint.getLineNum();
          } else {
            LOG.info("Checkpointed file " + partitionCheckpoint.getFileName()
                + " does not exist");
            startFromNextHigher((HadoopStreamFile) partitionCheckpoint.getStreamFile());
            return true;
          }
        } else {
          currentLineNum = partitionCheckpoint.getLineNum();
        }
        ret = true;
      }
    }
    this.currentFile = fileToRead;
    return ret;
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
      Integer currentMinute = cal.get(Calendar.MINUTE);
      if (partitionMinList.contains(currentMinute)) {
        // create a checkpoint for that minute only if it does not have
        // checkpoint or checkpoint file time stamp is older than
        // current file time stamp
        Date checkpointedTimeStamp = pChkpoints.get(currentMinute).timeStamp;
        if (checkpointedTimeStamp == null
            || checkpointedTimeStamp.before(cal.getTime())) {
          deltaCheckpoint.put(currentMinute, new PartitionCheckpoint
              (getStreamFile(cal.getTime()), -1));
          pChkpoints.get(currentMinute).processed = true;
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

  protected void startFromNextHigher(HadoopStreamFile file)
      throws IOException, InterruptedException {
    if (!setNextHigherAndOpen(file)) {
      waitForNextFileCreation(file);
    }
  }

  private void waitForNextFileCreation(HadoopStreamFile file)
      throws IOException, InterruptedException {
    while (!closed && !setNextHigherAndOpen(file) && !hasReadFully()) {
      waitForFileCreate();
      build();
    }
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
      waitForFileCreate();
      build();
    }
  }

  @Override
  public Message readLine() throws IOException, InterruptedException {
    Message line = readNextLine();
    if (!createdDeltaCheckpointForFirstFile) {
      // prepare partition checkpoint for all minutes in the partitionMinList
      deltaCheckpoint.putAll(buildStartPartitionCheckpoints());
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
            /* create  a delta checkpoint till stop time from last file timestamp
             * Ex: If last file is in 2nd hr, 5th minute(02/05) and stop time is 02/10
             * then we should have a delta checkpoint 02/05/file--1,
             *  02/06/null--1 till 02/10/null--1
             */
            Date lastFileTimestamp = getDateFromStreamDir(streamDir, getCurrentFile());
            if (stopTime != null) {
              setDeltaCheckpoint(getNextMinuteTimeStamp(lastFileTimestamp),
                  getNextMinuteTimeStamp(stopTime));
              currentLineNum = -1;
            }
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
    return getDateFromCheckpointPath(partitionCheckpoint.getFileName());
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
        Integer.valueOf(minute)).pck;
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

  public Map<Integer, PartitionCheckpoint> buildStartPartitionCheckpoints() {
    Map<Integer, PartitionCheckpoint> fullPartitionChkMap =
        new HashMap<Integer, PartitionCheckpoint>();
    if (buildTimestamp == null) {
      return fullPartitionChkMap;
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(buildTimestamp);
    cal.add(Calendar.MINUTE, 60);
    Date to = cal.getTime();
    cal.setTime(buildTimestamp);
    while (cal.getTime().before(to)) {
      Integer currentMinute = cal.get(Calendar.MINUTE);
      if (partitionMinList.contains(currentMinute)) {
        Date checkpointedTimeStamp = pChkpoints.get(currentMinute).timeStamp;
        // create a checkpoint for that minute only if it does not have
        // checkpoint so that for no minute the checkpoint is null
        if (checkpointedTimeStamp == null) {
          fullPartitionChkMap.put(currentMinute, new PartitionCheckpoint
              (getStreamFile(cal.getTime()), 0));
          pChkpoints.get(currentMinute).processed = true;
        }
      }
      cal.add(Calendar.MINUTE, 1);
    }
    return fullPartitionChkMap;
  }

  public void startFromCheckPoint() throws IOException, InterruptedException {
    /* If the partition checkpoint is completed checkpoint (i.e. line
    number is -1) or if it the filename of the checkpoint is null (
    when the checkpointing was done partially or before a single
    message was read) then it has to start from the next checkpoint.
    if (leastPartitionCheckpoint.getLineNum() == -1
        || leastPartitionCheckpoint.getName() == null) {
      //TODO. what about initFromNextCheckPoint returning null?
      ((DatabusStreamWaitingReader) reader).initFromNextCheckPoint();
    } else if (!reader.initializeCurrentFile(leastPartitionCheckpoint)) {
      reader.startFromNextHigher(leastPartitionCheckpoint.getFileName());
    }
     */
    initCurrentFile();
    moveToCheckpoint(getFirstFileInStream());
    if (currentFile != null) {
      LOG.debug("CurrentFile:" + getCurrentFile() + " currentLineNum:"
          + currentLineNum);
      setIterator();
    }
  }
}
