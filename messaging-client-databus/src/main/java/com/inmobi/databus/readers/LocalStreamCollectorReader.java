package com.inmobi.databus.readers;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.files.CollectorFile;
import com.inmobi.databus.files.DatabusStreamFile;
import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.mapred.DatabusInputFormat;
import com.inmobi.messaging.metrics.CollectorReaderStatsExposer;

public class LocalStreamCollectorReader extends
    DatabusStreamReader<DatabusStreamFile> {

  protected final String streamName;

  private static final Log LOG = LogFactory.getLog(
      LocalStreamCollectorReader.class);

  private final String collector;

  public LocalStreamCollectorReader(PartitionId partitionId,
      FileSystem fs, String streamName, Path streamDir, Configuration conf,
      long waitTimeForFileCreate, CollectorReaderStatsExposer metrics,
      Date stopTime)
          throws IOException {
    super(partitionId, fs, streamDir,
        DatabusInputFormat.class.getCanonicalName(), conf, waitTimeForFileCreate,
        metrics, false, stopTime);
    this.stopTime = stopTime;
    this.streamName = streamName;
    this.collector = partitionId.getCollector();
  }

  @Override
  protected void doRecursiveListing(Path dir, PathFilter pathFilter,
      FileMap<DatabusStreamFile> fmap) throws IOException {
    FileStatus[] fileStatuses = fsListFileStatus(dir, pathFilter);
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("No files in directory:" + dir);
    } else {
      for (FileStatus file : fileStatuses) {
        if (file.isDir()) {
          doRecursiveListing(file.getPath(), pathFilter, fmap);
        } else {
          try {
            Date currentTimeStamp = LocalStreamCollectorReader.
                getDateFromStreamFile(streamName, file.getPath().getName());
            if (stopTime != null && stopTime.before(currentTimeStamp)) {
              LOG.info("stopTime [ " + stopTime + " ] " + "is beyond the"
                  + " current file timestamp [ " + currentTimeStamp + " ]");
              stopListing();
            } else {
              fmap.addPath(file);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  protected void buildListing(FileMap<DatabusStreamFile> fmap, PathFilter pathFilter)
      throws IOException {
    if (!setBuildTimeStamp(null)) {
      return;
    }
    Calendar current = Calendar.getInstance();
    Date now = current.getTime();
    current.setTime(buildTimestamp);
    // stop the file listing if stop date is beyond current time
    while (current.getTime().before(now) && !isListingStopped()) {
      Path hhDir =  getHourDirPath(streamDir, current.getTime());
      int hour = current.get(Calendar.HOUR_OF_DAY);
      if (fsIsPathExists(hhDir)) {
        while (current.getTime().before(now)
            && hour  == current.get(Calendar.HOUR_OF_DAY) && !isListingStopped()) {
          Path dir = getMinuteDirPath(streamDir, current.getTime());
          // Move the current minute to next minute
          current.add(Calendar.MINUTE, 1);
          doRecursiveListing(dir, pathFilter, fmap);
        }
      } else {
        // go to next hour
        LOG.info("Hour directory " + hhDir + " does not exist");
        current.add(Calendar.HOUR_OF_DAY, 1);
        current.set(Calendar.MINUTE, 0);
      }
    }
  }

  @Override
  protected DatabusStreamFile getStreamFile(Date timestamp) {
    return getDatabusStreamFile(streamName, timestamp);
  }

  protected DatabusStreamFile getStreamFile(FileStatus status) {
    return DatabusStreamFile.create(streamName, status.getPath().getName());
  }

  public FileMap<DatabusStreamFile> createFileMap() throws IOException {
    return new FileMap<DatabusStreamFile>() {
      @Override
      protected void buildList() throws IOException {
        buildListing(this, pathFilter);
      }

      @Override
      protected TreeMap<DatabusStreamFile, FileStatus> createFilesMap() {
        return new TreeMap<DatabusStreamFile, FileStatus>();
      }

      @Override
      protected DatabusStreamFile getStreamFile(String fileName) {
        return DatabusStreamFile.create(streamName, fileName);
      }

      @Override
      protected DatabusStreamFile getStreamFile(FileStatus file) {
        return DatabusStreamFile.create(streamName, file.getPath().getName());
      }

      @Override
      protected PathFilter createPathFilter() {
        return new PathFilter() {
          @Override
          public boolean accept(Path p) {
            if (p.getName().startsWith(collector)) {
              return true;
            }
            return false;
          }
        };
      }
    };
  }

  public Message readLine() throws IOException {
    Message line = readNextLine();
    while (line == null) { // reached end of file
      if (closed) {
        LOG.info("Stream closed");
        break;
      }
      LOG.info("Read " + getCurrentFile() + " with lines:" + currentLineNum);
      if (!nextFile()) { // reached end of file list
        LOG.info("could not find next file. Rebuilding");
        build(getDateFromDatabusStreamFile(streamName,
            getCurrentFile().getName()));
        if (!setIterator()) {
          LOG.info("Could not find current file in the stream");
          // set current file to next higher entry
          if (!setNextHigherAndOpen(currentFile)) {
            LOG.info("Could not find next higher entry for current file");
            return null;
          } else {
            // read line from next higher file
            LOG.info("Reading from " + getCurrentFile()
                + ". The next higher file after rebuild");
          }
        } else if (!nextFile()) { // reached end of stream
          LOG.info("Reached end of stream");
          return null;
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

  public static Date getBuildTimestamp(String streamName, String collectorName,
      PartitionCheckpoint partitionCheckpoint) {
    String fileName = null;
    try {
      fileName = partitionCheckpoint.getFileName();
      if (fileName != null) {
        if (!isDatabusStreamFile(streamName, fileName)) {
          fileName = getDatabusStreamFileName(collectorName, fileName);
        }
        return getDateFromStreamFile(streamName, fileName);
      }
      return null;
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid fileName:" + fileName, e);
    }
  }

  static Date getDateFromDatabusStreamFile(String streamName, String fileName) {
    return DatabusStreamFile.create(streamName, fileName).getCollectorFile()
        .getTimestamp();
  }

  static Date getDateFromStreamFile(String streamName,
      String fileName) throws Exception {
    return getDatabusStreamFileFromLocalStreamFile(streamName, fileName).
        getCollectorFile().getTimestamp();
  }

  public static String getDatabusStreamFileName(String streamName,
      Date date) {
    return getDatabusStreamFile(streamName, date).toString();
  }

  public static DatabusStreamFile getDatabusStreamFile(String streamName,
      Date date) {
    return new DatabusStreamFile("", new CollectorFile(streamName, date, 0),
        "gz");
  }

  public static DatabusStreamFile getDatabusStreamFileFromLocalStreamFile(
      String streamName,
      String localStreamfileName) {
    return DatabusStreamFile.create(streamName, localStreamfileName);
  }

  static boolean isDatabusStreamFile(String streamName, String fileName) {
    try {
      getDatabusStreamFileFromLocalStreamFile(streamName, fileName);
    } catch (IllegalArgumentException ie) {
      return false;
    }
    return true;
  }

  public static String getDatabusStreamFileName(String collector,
      String collectorFile) {
    return getDatabusStreamFile(collector, collectorFile).toString();
  }

  public static DatabusStreamFile getDatabusStreamFile(String collector,
      String collectorFileName) {
    return new DatabusStreamFile(collector,
        CollectorFile.create(collectorFileName), "gz");
  }

}
