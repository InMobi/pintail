package com.inmobi.databus.readers;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
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
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class DatabusStreamWaitingReader 
    extends DatabusStreamReader<HadoopStreamFile> {

  private static final Log LOG = LogFactory.getLog(
      DatabusStreamWaitingReader.class);

  private int currentMin;
  private final Set<Integer> partitionMinList;
  private PartitionCheckpointList partitionCheckpointList;
  private boolean movedToNext;
  private int prevMin;

  public DatabusStreamWaitingReader(PartitionId partitionId, FileSystem fs,
      Path streamDir,  String inputFormatClass, Configuration conf,
      long waitTimeForFileCreate, PartitionReaderStatsExposer metrics,
      boolean noNewFiles, Set<Integer> partitionMinList, 
      PartitionCheckpointList partitionCheckpointList)
          throws IOException {
    super(partitionId, fs, streamDir, inputFormatClass, conf,
        waitTimeForFileCreate, metrics, noNewFiles);
    this.partitionCheckpointList = partitionCheckpointList;
    this.partitionMinList = partitionMinList; 
    currentMin = -1;
  }

  public boolean isRead(Date currentTimeStamp, int minute) {
    PartitionCheckpoint partitionCheckpoint = null;
    if (partitionCheckpointList != null && (partitionCheckpointList.
        getCheckpoints() != null)) {
      partitionCheckpoint = partitionCheckpointList.getCheckpoints().get(minute);
      if (partitionCheckpoint != null) {
        Date checkpointedTimestamp = getDateFromCheckpointPath(
            partitionCheckpoint.getFileName());
        if (currentTimeStamp.before(checkpointedTimestamp)) {  
          return true;
        } else if (currentTimeStamp.equals(checkpointedTimestamp)) {
          if (partitionCheckpoint.getLineNum() == -1) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public boolean initFromNextCheckPoint() throws IOException {
    initCurrentFile();
    currentFile = getFirstFileInStream();
    Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        currentFile.getPath().getParent());
    Calendar current = Calendar.getInstance();
    current.setTime(date);
    int currentMinute =current.get(Calendar.MINUTE);
    PartitionCheckpoint partitioncheckpoint = partitionCheckpointList.
        getCheckpoints().get(currentMinute);

    if (partitioncheckpoint != null) {
      Path checkpointedFileName =new Path(streamDir, 
          partitioncheckpoint.getFileName());
      if (!(currentFile.getPath()).equals(checkpointedFileName)) {
        if(fs.exists(checkpointedFileName)) {
          currentFile = fs.getFileStatus(checkpointedFileName);
          currentLineNum = partitioncheckpoint.getLineNum();
        } else {
          currentLineNum = 0;
        }
      } else {
        currentLineNum = partitioncheckpoint.getLineNum();
      }
    }
    if (currentFile != null) {
      LOG.debug("CurrentFile:" + getCurrentFile() + " currentLineNum:" +
          currentLineNum);
      setIterator();
    }
    return currentFile != null;
  }

  @Override
  protected void buildListing(FileMap<HadoopStreamFile> fmap,
      PathFilter pathFilter) throws IOException {
    Calendar current = Calendar.getInstance();
    Date now = current.getTime();
    current.setTime(buildTimestamp);
    boolean breakListing = false;
    while (current.getTime().before(now)) {
      Path hhDir =  getHourDirPath(streamDir, current.getTime());
      int hour = current.get(Calendar.HOUR_OF_DAY);
      if (fs.exists(hhDir)) {
        while (current.getTime().before(now) && 
            hour  == current.get(Calendar.HOUR_OF_DAY)) {
          Path dir = getMinuteDirPath(streamDir, current.getTime());
          int min = current.get(Calendar.MINUTE);
          Date currenTimestamp = current.getTime();
          current.add(Calendar.MINUTE, 1);
          if (fs.exists(dir)) {
            // Move the current minute to next minute
            Path nextMinDir = getMinuteDirPath(streamDir, current.getTime());
            if (partitionMinList.contains(new Integer(min))) {
              if (fs.exists(nextMinDir)) {
                if (!isRead(currenTimestamp, min)) {                                         
                  doRecursiveListing(dir, pathFilter, fmap);
                }
              } else {
                LOG.info("Reached end of file listing. Not looking at the last" +
                    " minute directory:" + dir);
                breakListing = true;
                break;
              }
            } 
          }
        } 
      } else {
        // go to next hour
        LOG.info("Hour directory " + hhDir + " does not exist");
        current.add(Calendar.HOUR_OF_DAY, 1);
        current.set(Calendar.MINUTE, 0);
      }
      if (breakListing) {
        break;
      }
    }

    if (getFirstFileInStream() != null && (currentMin == -1)) {
      Calendar cal = Calendar.getInstance();
      Date currentDate = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
          getFirstFileInStream().getPath().getParent());
      cal.setTime(currentDate);
      currentMin = cal.get(Calendar.MINUTE);
    }
  }

  @Override
  public boolean prepareMoveToNext(FileStatus currentFile, FileStatus nextFile) 
      throws IOException {                              
    Date date = getDateFromStreamDir(streamDir, currentFile.getPath().getParent());
    Calendar now = Calendar.getInstance();
    now.setTime(date);
    currentMin = now.get(Calendar.MINUTE);

    date = getDateFromStreamDir(streamDir, nextFile.getPath().getParent());
    now.setTime(date);

    boolean readFromCheckpoint = false;
    FileStatus fileToRead = nextFile;
    if (currentMin != now.get(Calendar.MINUTE)) {
      //We are moving to next file, set the flags so that Message checkpoints 
      //can be populated.
      movedToNext = true;
      prevMin = currentMin;
      currentMin = now.get(Calendar.MINUTE);
      PartitionCheckpoint partitionCheckpoint = partitionCheckpointList.
          getCheckpoints().get(currentMin);
      if (partitionCheckpoint != null && partitionCheckpoint.getLineNum() != -1)
      {
        Path checkPointedFileName = new Path(streamDir, 
            partitionCheckpoint.getFileName());
        //set iterator to checkpoointed file if there is a checkpoint
        if(!fileToRead.getPath().equals(checkPointedFileName)) {
          if (fs.exists(checkPointedFileName)) {
            fileToRead = fs.getFileStatus(checkPointedFileName);
            currentLineNum = partitionCheckpoint.getLineNum();
          } else {
            currentLineNum = 0;
          }
        } else {
          currentLineNum = partitionCheckpoint.getLineNum();
        }
        readFromCheckpoint = true;
      }
      updatePartitionCheckpointList(prevMin);
    }
    this.currentFile = fileToRead;
    setIterator();
    return !readFromCheckpoint;
  }  

  private void updatePartitionCheckpointList(int prevMin) {
    Map<Integer, PartitionCheckpoint> pckList = partitionCheckpointList.
        getCheckpoints();
    pckList.remove(prevMin);
    partitionCheckpointList.setCheckpoint(pckList);
  }

  @Override
  protected HadoopStreamFile getStreamFile(Date timestamp) {
    return new HadoopStreamFile(getMinuteDirPath(streamDir, timestamp),
        null, null);
  }

  protected HadoopStreamFile getStreamFile(FileStatus status) {
    return getHadoopStreamFile(status);
  }

  protected void startFromNextHigher(FileStatus file)
      throws IOException, InterruptedException {
    if (!setNextHigherAndOpen(file)) {
      if (noNewFiles) {
        // this boolean check is only for tests 
        return;
      }
      waitForNextFileCreation(file);
    }
  }

  private void waitForNextFileCreation(FileStatus file)
      throws IOException, InterruptedException {
    while (!closed && !setNextHigherAndOpen(file)) {
      LOG.info("Waiting for next file creation");
      waitForFileCreate();
      build();
    }
  }

  @Override
  public byte[] readLine() throws IOException, InterruptedException {
    byte[] line = readNextLine();
    while (line == null) { // reached end of file
      LOG.info("Read " + getCurrentFile() + " with lines:" + currentLineNum);
      if (closed) {
        LOG.info("Stream closed");
        break;
      }
      if (!nextFile()) { // reached end of file list
        LOG.info("could not find next file. Rebuilding");
        build(getDateFromStreamDir(streamDir, 
            getCurrentFile()));
        if (!nextFile()) { // reached end of stream
          if (noNewFiles) {
            // this boolean check is only for tests 
            return null;
          } 
          LOG.info("Could not find next file");
          startFromNextHigher(currentFile);
          LOG.info("Reading from next higher file "+ getCurrentFile());
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
      throw new IllegalArgumentException("Invalid checkpoint:" + 
          partitionCheckpoint.getStreamFile(), e);
    }
  }

  public static HadoopStreamFile getHadoopStreamFile(FileStatus status) {
    return HadoopStreamFile.create(status);
  }

  public void resetMoveToNextFlags() {
    movedToNext = false;
    prevMin = -1;
  }

  public boolean isMovedToNext() {
    return movedToNext;
  }

  public int getPrevMin() {
    return this.prevMin;
  }

  public int getCurrentMin() {
    return this.currentMin;
    
  }
}