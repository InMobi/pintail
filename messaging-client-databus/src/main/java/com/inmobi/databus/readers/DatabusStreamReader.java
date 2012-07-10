package com.inmobi.databus.readers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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

public abstract class DatabusStreamReader<T extends StreamFile> extends 
    StreamReader<T> {

  private static final Log LOG = LogFactory.getLog(DatabusStreamReader.class);

  private FileSplit currentFileSplit;
  private RecordReader<Object, Object> recordReader;
  private InputFormat<Object, Object> input;
  private Configuration conf;
  private Date buildTimestamp;
  
  protected DatabusStreamReader(PartitionId partitionId, FileSystem fs,
      String streamName, Path streamDir, String inputFormatClass,
      Configuration conf, boolean noNewFiles)
          throws IOException {
    super(partitionId, fs, streamName, streamDir, noNewFiles);
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

  void buildListing(FileMap<T> fmap, PathFilter pathFilter)
      throws IOException {
    Calendar current = Calendar.getInstance();
    Date now = current.getTime();
    current.setTime(buildTimestamp);
    while (current.getTime().before(now)) {
      Path hhDir =  getHourDirPath(streamDir, current.getTime());
      int hour = current.get(Calendar.HOUR_OF_DAY);
      if (fs.exists(hhDir)) {
        while (current.getTime().before(now) && 
            hour  == current.get(Calendar.HOUR_OF_DAY)) {
          Path dir = getMinuteDirPath(streamDir, current.getTime());
          // Move the current minute to next minute
          current.add(Calendar.MINUTE, 1);
          FileStatus[] fileStatuses = fs.listStatus(dir, pathFilter);
          if (fileStatuses == null || fileStatuses.length == 0) {
            LOG.debug("No files in directory:" + dir);
          } else {
            for (FileStatus file : fileStatuses) {
              fmap.addPath(file);
            }
          }
        } 
      } else {
        // go to next hour
        LOG.info("Hour directory " + hhDir + " does not exist");
        current.add(Calendar.HOUR_OF_DAY, 1);
        current.set(Calendar.MINUTE, 0);
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
      LOG.info("Could not find checkpointed file: " + checkpoint.getStreamFile());
      if (isBeforeStream((T)checkpoint.getStreamFile())) {
        LOG.info("Reading from start of the stream");
        return initFromStart();
      } else {
        LOG.info("The checkpoint is not before the stream. Ignoring it");
      }
    }
    return ret;
  }

  protected void openCurrentFile(boolean next) throws IOException {
    closeCurrentFile();
    if (next) {
      resetCurrentFileSettings();
    }
    LOG.info("Opening file:" + getCurrentFile() + " NumLinesTobeSkipped when" +
        " opening:" + currentLineNum);
    try {
      FileStatus status = fs.getFileStatus(getCurrentFile());
      if (status != null) {
        currentFileSplit = new FileSplit(getCurrentFile(), 0L,
            status.getLen(), new String[0]);
        recordReader = input.getRecordReader(currentFileSplit, new JobConf(conf),
            Reporter.NULL);
        skipLines(currentLineNum);
      } else {
        LOG.info("CurrentFile:" + getCurrentFile() + " does not exist");        
      }
    } catch (FileNotFoundException fnfe) {
      LOG.info("CurrentFile:" + getCurrentFile() + " does not exist");
    }
  }

  protected synchronized void closeCurrentFile() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
    }
    currentFileSplit = null;
  }

  protected String readRawLine() throws IOException {
    if (recordReader != null) {
      Object key = recordReader.createKey();
      Object value = recordReader.createValue();
      boolean ret = recordReader.next(key, value);
      if (ret) {
        return value.toString();
      }
    }
    return null;
  }

  protected boolean setNextHigherAndOpen(FileStatus currentFile)
      throws IOException {
    LOG.debug("finding next higher for " + getCurrentFile());
    FileStatus nextHigherFile  = getHigherValue(currentFile);
    boolean ret = setIteratorToFile(nextHigherFile);
    if (ret) {
      openCurrentFile(true);
    }
    return ret;
  }

  public static Date getDateFromStreamDir(Path streamDir, Path dir) {
    String pathStr = dir.toString();
    String dirString = pathStr.substring(streamDir.toString().length() + 1);
    try {
      return minDirFormat.get().parse(dirString);
    } catch (ParseException e) {
      LOG.warn("Could not get date from directory passed", e);
    }
    return null;
  }

  static final ThreadLocal<DateFormat> minDirFormat = 
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + File.separator + "MM" +
          File.separator + "dd" + File.separator + "HH" + File.separator +"mm");
    }    
  };

  static final ThreadLocal<DateFormat> hhDirFormat = 
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + File.separator + "MM" +
          File.separator + "dd" + File.separator + "HH");
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
}
