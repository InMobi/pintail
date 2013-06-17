package com.inmobi.databus.readers;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public abstract class DatabusStreamReader<T extends StreamFile> extends
StreamReader<T> {

  private static final Log LOG = LogFactory.getLog(DatabusStreamReader.class);

  private FileSplit currentFileSplit;
  private RecordReader<Object, Object> recordReader;
  private InputFormat<Object, Object> input;
  private Configuration conf;
  protected Date buildTimestamp;
  private Object msgKey;
  private Object msgValue;
  private ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private boolean needsSerialize;

  protected DatabusStreamReader(PartitionId partitionId, FileSystem fs,
      Path streamDir, String inputFormatClass,
      Configuration conf, long waitTimeForFileCreate,
      PartitionReaderStatsExposer metrics, boolean noNewFiles, Date stopTime)
          throws IOException {
    super(partitionId, fs, streamDir, waitTimeForFileCreate, metrics,
        noNewFiles, stopTime);
    this.conf = conf;
    try {
      input = (InputFormat<Object, Object>) ReflectionUtils.newInstance(
          conf.getClassByName(inputFormatClass), conf);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Input format class"
          + inputFormatClass + " not found", e);
    }
  }

  public void build(Date date) throws IOException {
    this.buildTimestamp = date;
    build();
  }

  protected abstract void buildListing(FileMap<T> fmap, PathFilter pathFilter)
      throws IOException;

  protected void doRecursiveListing(Path dir, PathFilter pathFilter,
      FileMap<T> fmap) throws IOException {
    FileStatus[] fileStatuses = fsListFileStatus(dir, pathFilter);
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("No files in directory:" + dir);
    } else {
      for (FileStatus file : fileStatuses) {
        if (file.isDir()) {
          doRecursiveListing(file.getPath(), pathFilter, fmap);
        } else {
          fmap.addPath(file);
        }
      }
    }
  }

  /**
   *  Comment out this method if partition reader should not read from start of
   *   stream
   *  if check point does not exist.
   */
  public boolean initializeCurrentFile(PartitionCheckpoint checkpoint)
      throws IOException {
    boolean ret = super.initializeCurrentFile(checkpoint);
    if (!ret) {
      T streamFile = (T) checkpoint.getStreamFile();
      LOG.info("Could not find checkpointed file: " + streamFile);
      if (isBeforeStream(streamFile)) {
        LOG.info("Reading from start of the stream");
        return initFromStart();
      } else {
        LOG.info("The checkpoint is not before the stream. Ignoring it");
      }
    }
    return ret;
  }

  protected boolean openCurrentFile(boolean next) throws IOException {
    closeCurrentFile();
    if (getCurrentFile() == null) {
      return false;
    }
    if (next) {
      resetCurrentFileSettings();
    }
    LOG.info("Opening file:" + getCurrentFile() + " NumLinesTobeSkipped when"
        + " opening:" + currentLineNum);
    try {
      FileStatus status = fsGetFileStatus(getCurrentFile());
      if (status != null) {
        currentFileSplit = new FileSplit(getCurrentFile(), 0L,
            status.getLen(), new String[0]);
        recordReader = input.getRecordReader(currentFileSplit, new JobConf(conf),
            Reporter.NULL);
        metrics.incrementNumberRecordReaders();
        msgKey = recordReader.createKey();
        msgValue = recordReader.createValue();
        if (msgValue instanceof Writable) {
          needsSerialize = true;
        } else {
          assert (msgValue instanceof Message);
          needsSerialize = false;
        }
        skipLines(currentLineNum);
      } else {
        LOG.info("CurrentFile:" + getCurrentFile() + " does not exist");
      }
    } catch (FileNotFoundException fnfe) {
      LOG.info("CurrentFile:" + getCurrentFile() + " does not exist");
    }
    return true;
  }

  protected synchronized void closeCurrentFile() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
    }
    currentFileSplit = null;
  }

  protected Message readRawLine() throws IOException {
    if (recordReader != null) {
      if (!needsSerialize) {
        msgValue = recordReader.createValue();
      }
      boolean ret = recordReader.next(msgKey, msgValue);
      if (ret) {
        if (needsSerialize) {
          baos.reset();
          ((Writable) msgValue).write(new DataOutputStream(baos));
          return new Message(baos.toByteArray());
        } else {
          return ((Message) msgValue);
        }
      }
    }
    return null;
  }

  protected boolean setNextHigherAndOpen(FileStatus currentFile)
      throws IOException {
    LOG.debug("finding next higher for " + getCurrentFile());
    FileStatus nextHigherFile  = getHigherValue(currentFile);
    boolean next = true;
    if (nextHigherFile != null) {
      next = prepareMoveToNext(currentFile, nextHigherFile);
    }
    boolean ret = setIteratorToFile(nextHigherFile);
    if (ret) {
      openCurrentFile(next);
    }
    return ret;
  }

  public static Date getDateFromStreamDir(Path streamDir, Path dir) {
    String pathStr = dir.toString();
    int startIndex = streamDir.toString().length() + 1;
    String dirString = pathStr.substring(startIndex,
        startIndex + minDirFormatStr.length());
    try {
      return minDirFormat.get().parse(dirString);
    } catch (ParseException e) {
      LOG.warn("Could not get date from directory passed", e);
    }
    return null;
  }

  public static Date getDateFromCheckpointPath(String checkpointPath) {
    String dirString = checkpointPath.substring(0, minDirFormatStr.length());
    try {
      return minDirFormat.get().parse(dirString);
    } catch (ParseException e) {
      LOG.warn("Could not get date from directory passed", e);
    }
    return null;
  }

  static String minDirFormatStr = "yyyy" + File.separator + "MM"
      + File.separator + "dd" + File.separator + "HH" + File.separator + "mm";

  static final ThreadLocal<DateFormat> minDirFormat =
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat(minDirFormatStr);
    }
  };

  static final ThreadLocal<DateFormat> hhDirFormat =
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + File.separator + "MM"
          + File.separator + "dd" + File.separator + "HH");
    }
  };

  public static Path getStreamsLocalDir(Cluster cluster, String streamName) {
    return new Path(cluster.getLocalFinalDestDirRoot(), streamName);
  }

  public static Path getStreamsDir(Cluster cluster, String streamName) {
    return new Path(cluster.getFinalDestDirRoot(), streamName);
  }

  public static Path getHourDirPath(Path streamDir, Date date) {
    return new Path(streamDir, hhDirFormat.get().format(date));
  }

  public static Path getMinuteDirPath(Path streamDir, Date date) {
    return new Path(streamDir, minDirFormat.get().format(date));
  }

  protected boolean setBuildTimeStamp(PathFilter pathFilter) throws IOException {
    if (buildTimestamp == null) {
      Date tmp = getTimestampFromStartOfStream(pathFilter);
      if (tmp != null) {
        this.buildTimestamp = tmp;
      } else {
        LOG.info("Could not find start directory yet");
        return false;
      }
    }
    return true;
  }

  protected Date getTimestampFromStartOfStream(PathFilter pathFilter)
      throws IOException {
    FileStatus leastTimeStampFileStatus = null;
    Path dir = streamDir;
    for (int d = 0; d < 5; d++) {
      FileStatus [] filestatuses = fsListFileStatus(dir, pathFilter);
      if (filestatuses != null && filestatuses.length > 0) {
        leastTimeStampFileStatus = filestatuses[0];
        for (int i = 1; i < filestatuses.length; i++) {
          if (leastTimeStampFileStatus.getPath().
              compareTo(filestatuses[i].getPath()) > 0) {
            leastTimeStampFileStatus = filestatuses[i];
          }
        }
        dir = leastTimeStampFileStatus.getPath();
      } else {
        return null;
      }
    }
    return getDateFromStreamDir(streamDir, leastTimeStampFileStatus.getPath());
  }
}
