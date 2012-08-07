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

import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;

public class DatabusStreamWaitingReader 
     extends DatabusStreamReader<HadoopStreamFile> {

  private static final Log LOG = LogFactory.getLog(
      DatabusStreamWaitingReader.class);

  public DatabusStreamWaitingReader(PartitionId partitionId, FileSystem fs,
      Path streamDir,  String inputFormatClass, Configuration conf,
      long waitTimeForCreate, boolean noNewFiles)
          throws IOException {
    super(partitionId, fs, streamDir, inputFormatClass, conf, noNewFiles);
    this.waitTimeForCreate = waitTimeForCreate;
  }

  protected void buildListing(FileMap<HadoopStreamFile> fmap, PathFilter pathFilter)
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
          Path nextMinDir = getMinuteDirPath(streamDir, current.getTime());
          if (fs.exists(nextMinDir)) {
            doRecursiveListing(dir, pathFilter, fmap);
          } else {
            LOG.info("Reached end of file listing. Not looking at the last" +
                " minute directory:" + dir);
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
      Thread.sleep(waitTimeForCreate);
      build();
    }
  }

  @Override
  public byte[] readLine() throws IOException, InterruptedException {
    byte[] line = readNextLine();
    while (line == null) { // reached end of file
      if (closed) {
        LOG.info("Stream closed");
        break;
      }
      LOG.info("Read " + getCurrentFile() + " with lines:" + currentLineNum);
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
      return getDateFromStreamDir(streamDir,
          ((HadoopStreamFile)partitionCheckpoint.getStreamFile()).getParent());
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid checkpoint:" + 
          partitionCheckpoint.getStreamFile(), e);
    }
  }

  public static HadoopStreamFile getHadoopStreamFile(FileStatus status) {
    return HadoopStreamFile.create(status);
  }
}