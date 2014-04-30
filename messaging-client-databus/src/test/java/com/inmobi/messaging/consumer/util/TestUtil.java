package com.inmobi.messaging.consumer.util;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.testng.Assert;

import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.partition.DeltaPartitionCheckPoint;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.databus.readers.LocalStreamCollectorReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.Message;

public class TestUtil {
  static final Log LOG = LogFactory.getLog(TestUtil.class);

  private static final String testStream = "testclient";
  private static Map<ClusterUtil, Date> lastCommitTimes = new HashMap<ClusterUtil, Date>();

  public static String[] files = new String[12];
  private static int increment = 1;

  static {
    Calendar now = Calendar.getInstance();
    now.add(Calendar.MINUTE, - (files.length + 5));
    Date startTime = now.getTime();
    now.setTime(startTime);
    for (int i = 0; i < 12; i++) {
      startTime = now.getTime();
      files[i] = CollectorStreamReader.getCollectorFileName(testStream,
          startTime);
      LOG.debug("file:" + i + " :" + files[i]);
      now.add(Calendar.MINUTE, 1);
    }
  }

  public static void createEmptyFile(FileSystem fs, Path parent, String fileName)
      throws IOException {
    FSDataOutputStream out = fs.create(new Path(parent, fileName));
    LOG.debug("Created empty file:" + new Path(parent, fileName));
    out.close();
  }

  public static void incrementCommitTime() {
    increment++;
  }

  public static Path moveFileToStreamLocal(FileSystem fs, String streamName,
      String collectorName, ClusterUtil cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    return moveCollectorFile(fs, streamName, collectorName, cluster,
        collectorDir, collectorfileName, StreamType.LOCAL);
  }

  public static Path moveFileToStreams(FileSystem fs, String streamName,
      String collectorName, ClusterUtil cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    return moveCollectorFile(fs, streamName, collectorName, cluster,
        collectorDir, collectorfileName, StreamType.MERGED);
  }

  public static Path moveCollectorFile(FileSystem fs, String streamName,
      String collectorName, ClusterUtil cluster, Path collectorDir,
      String collectorfileName, StreamType streamType)
          throws Exception {
    Path targetFile = getTargetPath(fs, streamName, collectorName, cluster,
        collectorfileName, streamType);
    Path srcPath = copyCollectorFile(targetFile, cluster, collectorDir,
        collectorfileName);
    fs.delete(srcPath, true);
    return targetFile;
  }

  private static Path getTargetPath(FileSystem fs, String streamName,
      String collectorName, ClusterUtil cluster,
      String collectorfileName, StreamType streamType) throws IOException {
    String streamFileName = LocalStreamCollectorReader.getDatabusStreamFileName(
        collectorName, collectorfileName);
    Date commitTime = getCommitDateForCollectorFile(collectorfileName);
    Path streamDir = DatabusUtil.getStreamDir(streamType,
        new Path(cluster.getRootDir()), streamName);
    publishMissingPaths(fs, streamDir, lastCommitTimes.get(cluster), commitTime);
    lastCommitTimes.put(cluster, commitTime);
    Path streamMinDir = DatabusStreamReader.getMinuteDirPath(streamDir, commitTime);
    return new Path(streamMinDir, streamFileName);
  }

  private static Path copyCollectorFile(Path targetFile, ClusterUtil cluster,
      Path collectorDir, String collectorfileName) throws IOException {
    Path collectorPath = new Path(collectorDir, collectorfileName);
    FileUtil.gzip(collectorPath, targetFile, cluster.getHadoopConf());
    LOG.info("Copied " + collectorPath + " to" + targetFile);
    return collectorPath;
  }

  public static Path copyFileToStreamLocal(FileSystem fs, String streamName,
      String collectorName, ClusterUtil cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    Path targetFile = getTargetPath(fs, streamName, collectorName, cluster,
        collectorfileName, StreamType.LOCAL);

    copyCollectorFile(targetFile, cluster, collectorDir, collectorfileName);
    return targetFile;
  }

  public static Path copyFileToStreams(FileSystem fs, String streamName,
      String collectorName, ClusterUtil cluster, Path collectorDir,
      String collectorfileName)
          throws Exception {
    Path targetFile = getTargetPath(fs, streamName, collectorName, cluster,
        collectorfileName, StreamType.MERGED);

    copyCollectorFile(targetFile, cluster, collectorDir, collectorfileName);
    return targetFile;
  }

  public static void assertBuffer(StreamFile file, int fileNum, int startIndex,
      int numMsgs, PartitionId pid, LinkedBlockingQueue<QueueEntry> buffer,
      boolean isDatabusData, Map<Integer, PartitionCheckpoint> expectedDeltaPck)
          throws InterruptedException, IOException {

    int fileIndex = (fileNum - 1) * 100 ;
    for (int i = startIndex; i < (startIndex + numMsgs); i++) {
      QueueEntry entry = buffer.take();
      Assert.assertEquals(entry.getPartitionId(), pid);
      if (entry.getMessageChkpoint() instanceof DeltaPartitionCheckPoint) {
        int min = Integer.parseInt(new Path(file.toString()).getParent().getName());
        Map<Integer, PartitionCheckpoint> actualDeltaPck =
            ((DeltaPartitionCheckPoint) entry.getMessageChkpoint()).
            getDeltaCheckpoint();
        // get expected delta pck
        expectedDeltaPck = new DeltaPartitionCheckPoint(file, i + 1, min,
            expectedDeltaPck).getDeltaCheckpoint();
        // assert on expected and actual delta pck
        Assert.assertEquals(actualDeltaPck, expectedDeltaPck);
        expectedDeltaPck.clear();
      } else {
        Assert.assertEquals(entry.getMessageChkpoint(),
            new PartitionCheckpoint(file, i + 1));
      }
      if (isDatabusData) {
        Assert.assertEquals(new String(((Message) entry.getMessage()).getData().array()),
          MessageUtil.constructMessage(fileIndex + i));
      } else {
        Assert.assertEquals(MessageUtil.getTextMessage(
            ((Message) entry.getMessage()).getData().array()),
            new Text(MessageUtil.constructMessage(fileIndex + i)));
      }
    }
  }

  public static void prepareExpectedDeltaPck(Date fromTime, Date toTime,
      Map<Integer, PartitionCheckpoint> expectedDeltaPck, FileStatus file,
      Path streamDir, Set<Integer> partitionMinList,
      PartitionCheckpointList partitionCheckpointList, boolean isStart,
      boolean isFileCompleted) {
    Map<Integer, Date> chkTimeStampMap = new HashMap<Integer, Date>();
    // prepare a checkpoint map
    prepareChkpointTimeMap(chkTimeStampMap, partitionMinList,
        partitionCheckpointList);
    Calendar current = Calendar.getInstance();
    current.setTime(fromTime);
    int lineNum;
    if (isFileCompleted) {
      lineNum = -1;
    } else {
      lineNum = 0;
    }
    if(isStart) {
      Calendar hourCal = Calendar.getInstance();
      hourCal.setTime(fromTime);
      hourCal.add(Calendar.MINUTE, 60);
      Date hourTime = hourCal.getTime();
      while (current.getTime().before(hourTime)) {
        int minute = current.get(Calendar.MINUTE);
        if (partitionMinList.contains(minute)) {
          Date chkTime = chkTimeStampMap.get(minute);
          if (chkTime == null || chkTime.before(current.getTime())) {
            expectedDeltaPck.put(Integer.valueOf(minute),
                new PartitionCheckpoint(DatabusStreamWaitingReader.
                    getHadoopStreamFile(streamDir, current.getTime()),
                    lineNum));
          }
        }
        current.add(Calendar.MINUTE, 1);
      }
    }
    if(!isStart) {
      current.add(Calendar.MINUTE, 1);
      while (current.getTime().before(toTime)) {
        int minute = current.get(Calendar.MINUTE);
        if (partitionMinList.contains(minute)) {
          Date chkTime = chkTimeStampMap.get(minute);
          if (chkTime == null || !chkTime.after(current.getTime())) {
            expectedDeltaPck.put(Integer.valueOf(minute),
                new PartitionCheckpoint(DatabusStreamWaitingReader.
                    getHadoopStreamFile(streamDir, current.getTime()), -1));
          }
        }
        current.add(Calendar.MINUTE, 1);
      }
    }
    current.setTime(fromTime);
    if (file != null) {
      int minute = current.get(Calendar.MINUTE);
      expectedDeltaPck.put(Integer.valueOf(minute), new PartitionCheckpoint(
          DatabusStreamWaitingReader.getHadoopStreamFile(file), -1));
    } else {
      int minute = current.get(Calendar.MINUTE);
      expectedDeltaPck.put(Integer.valueOf(minute),
          new PartitionCheckpoint(DatabusStreamWaitingReader.
              getHadoopStreamFile(streamDir, current.getTime()), -1));
    }
  }

  public static void prepareChkpointTimeMap(Map<Integer, Date> chkTimeStampMap,
      Set<Integer> partitionMinList,
      PartitionCheckpointList partitionCheckpointList) {
    Map<Integer, PartitionCheckpoint> partitionChkList =
        partitionCheckpointList.getCheckpoints();
    for (Integer min : partitionMinList) {
      PartitionCheckpoint pck = partitionChkList.get(Integer.valueOf(min));
      if (pck != null) {
        Date timeStamp = DatabusStreamWaitingReader.getDateFromCheckpointPath(
            pck.getFileName());
        chkTimeStampMap.put(min, timeStamp);
      } else {
        chkTimeStampMap.put(min, null);
      }
    }
  }

  public static void setUpCollectorDataFiles(FileSystem fs, Path collectorDir,
      String... files) throws IOException {
    int i = 0;
    for (String file : files) {
      MessageUtil.createMessageFile(file, fs, collectorDir, i);
      i += 100;
    }
  }

  public static void setUpEmptyFiles(FileSystem fs, Path collectorDir,
      String... files) throws IOException {
    for (String file : files) {
      createEmptyFile(fs, collectorDir, file);
    }
  }

  public static ClusterUtil setupLocalCluster(String className, String testStream,
      PartitionId pid, String[] collectorFiles,
      String[] emptyFiles, Path[] databusFiles,
      int numFilesToMoveToStreamLocal, String testRootDir) throws Exception {
    return setupCluster(className, testStream, pid, "file:///", collectorFiles,
        emptyFiles, databusFiles, numFilesToMoveToStreamLocal, 0, testRootDir);
  }

  public static ClusterUtil setupLocalCluster(String className, String testStream,
      PartitionId pid, String[] collectorFiles,
      String[] emptyFiles,
      int numFilesToMoveToStreamLocal, String testRootDir) throws Exception {
    return setupCluster(className, testStream, pid, "file:///", collectorFiles,
        emptyFiles, null, numFilesToMoveToStreamLocal, 0, testRootDir);
  }

  public static ClusterUtil setupLocalCluster(String className, String testStream,
      PartitionId pid, String[] collectorFiles,
      String[] emptyFiles, Path[] databusFiles,
      int numFilesToMoveToStreamLocal, int numFilesToMoveToStreams, String testRootDir)
          throws Exception {
    return setupCluster(className, testStream, pid, "file:///", collectorFiles,
        emptyFiles, databusFiles, numFilesToMoveToStreamLocal,
        numFilesToMoveToStreams, testRootDir);
  }

  public static Path getCollectorDir(ClusterUtil cluster, String streamName,
      String collectorName) {
    Path streamDir = new Path(cluster.getDataDir(), streamName);
    return new Path(streamDir, collectorName);
  }

  private static ClusterUtil setupCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles,
      String[] emptyFiles, Path[] databusFiles,
      int numFilesToMoveToStreamLocal, int numFilesToMoveToStreams,
      String testRootDir)
          throws Exception {
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add(testStream);
    Map<String, String> clusterConf = new HashMap<String, String>();
    clusterConf.put("hdfsurl", hdfsUrl);
    clusterConf.put("jturl", "local");
    clusterConf.put("name", pid.getCluster());
    clusterConf.put("jobqueue", "default");
    
    ClusterUtil cluster = new ClusterUtil(clusterConf,
        new Path(testRootDir, className).toString(), sourceNames);

    // setup stream and collector dirs
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    Path collectorDir = getCollectorDir(cluster, testStream, pid.getCollector());
    fs.delete(collectorDir, true);
    fs.delete(new Path(cluster.getLocalFinalDestDirRoot()), true);
    fs.delete(new Path(cluster.getFinalDestDirRoot()), true);
    fs.mkdirs(collectorDir);

    setUpFiles(cluster, pid.getCollector(), collectorFiles, emptyFiles,
        databusFiles, numFilesToMoveToStreamLocal, numFilesToMoveToStreams);

    return cluster;
  }

  public static void setUpFiles(ClusterUtil cluster, String collectorName,
      String[] collectorFiles,
      String[] emptyFiles, Path[] databusFiles,
      int numFilesToMoveToStreamLocal, int numFilesToMoveToStreams)
          throws Exception {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    Path streamDir = new Path(cluster.getDataDir(), testStream);
    Path collectorDir = new Path(streamDir, collectorName);

    // setup data dirs
    if (collectorFiles != null) {
      TestUtil.setUpCollectorDataFiles(fs, collectorDir, collectorFiles);
    }

    if (emptyFiles != null) {
      TestUtil.setUpEmptyFiles(fs, collectorDir, emptyFiles);
    }

    if (numFilesToMoveToStreamLocal > 0 && collectorFiles != null) {
      for (int i = 0; i < numFilesToMoveToStreamLocal; i++) {
        if (numFilesToMoveToStreams > 0) {
          TestUtil.copyFileToStreamLocal(fs,
              testStream, collectorName,
              cluster, collectorDir, collectorFiles[i]);
        } else {
          Path movedPath = TestUtil.moveFileToStreamLocal(fs,
              testStream, collectorName,
              cluster, collectorDir, collectorFiles[i]);
          if (databusFiles != null) {
            databusFiles[i] = movedPath;
          }
        }
      }
      publishLastPath(fs, DatabusUtil.getStreamDir(StreamType.LOCAL,
          new Path(cluster.getRootDir()), testStream),
          lastCommitTimes.get(cluster));
    }

    if (numFilesToMoveToStreams > 0 && collectorFiles != null) {
      for (int i = 0; i < numFilesToMoveToStreams; i++) {
        Path movedPath = TestUtil.moveFileToStreams(fs,
            testStream, collectorName,
            cluster, collectorDir, collectorFiles[i]);
        if (databusFiles != null) {
          databusFiles[i] = movedPath;
        }
      }
      publishLastPath(fs, DatabusUtil.getStreamDir(StreamType.MERGED,
          new Path(cluster.getRootDir()), testStream), lastCommitTimes.get(cluster));
    }
  }

  public static ClusterUtil setupDFSCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles,
      String[] emptyFiles, Path[] databusFiles,
      int numFilesToMoveToStreamLocal, int numFilesToMoveToStreams,
      String testRootDir)
          throws Exception {
    return setupCluster(className, testStream, pid, hdfsUrl, collectorFiles,
        emptyFiles, databusFiles, numFilesToMoveToStreamLocal,
        numFilesToMoveToStreams, testRootDir);
  }

  public static ClusterUtil setupDFSCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles,
      String[] emptyFiles, int numFilesToMoveToStreamLocal,
      String testRootDir) throws Exception {
    return setupDFSCluster(className, testStream, pid, hdfsUrl, collectorFiles,
        emptyFiles, null, numFilesToMoveToStreamLocal, 0, testRootDir);
  }

  public static void cleanupCluster(ClusterUtil cluster) throws IOException {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    LOG.debug("Cleaning up the dir: " + cluster.getRootDir());
    fs.delete(new Path(cluster.getRootDir()), true);
  }

  static void publishMissingPaths(FileSystem fs, Path baseDir,
      Date lastCommitTime, Date uptoCommit) throws IOException {
    LOG.debug("publishMissingPaths lastCommitTime:" + lastCommitTime
        + " uptoCommit:" + uptoCommit);
    if (lastCommitTime != null) {
      Calendar cal = Calendar.getInstance();
      cal.setTime(lastCommitTime);
      cal.add(Calendar.MINUTE, 1);
      while (cal.getTime().before(uptoCommit)) {
        Path minDir = DatabusStreamReader.getMinuteDirPath(baseDir,
            cal.getTime());
        fs.mkdirs(minDir);
        LOG.info("Created minDir:" + minDir);
        cal.add(Calendar.MINUTE, 1);
      }
    } else {
      LOG.info("Nothing to publish");
    }
  }

  static void publishLastPath(FileSystem fs, Path baseDir,
      Date lastCommitTime) throws IOException {
    if (lastCommitTime != null) {
      Calendar cal = Calendar.getInstance();
      cal.setTime(lastCommitTime);
      cal.add(Calendar.MINUTE, 1);
      Path minDir = DatabusStreamReader.getMinuteDirPath(baseDir,
          cal.getTime());
      fs.mkdirs(minDir);
      LOG.info("Created minDir:" + minDir);
    }
  }

  public static void publishLastPathForStreamsDir(FileSystem fs,
      ClusterUtil cluster, String streamName) throws IOException {
    publishLastPath(fs, DatabusUtil.getStreamDir(StreamType.MERGED,
        new Path(cluster.getRootDir()), streamName),
        lastCommitTimes.get(cluster));
  }

  static Date getCommitDateForCollectorFile(String fileName)
      throws IOException {
    Calendar cal = Calendar.getInstance();
    Date date = CollectorStreamReader.getDateFromCollectorFile(fileName);
    cal.setTime(date);
    cal.add(Calendar.MINUTE, increment);
    return cal.getTime();
  }

  public static String getConfiguredRootDir() {
    return System.getProperty("test.root.dir", "/tmp/test");
  }
  
  public static Path getStreamsLocalDir(ClusterUtil cluster, String streamName) {
    return new Path(cluster.getLocalFinalDestDirRoot(), streamName);
  }

  public static Path getStreamsDir(ClusterUtil cluster, String streamName) {
    return new Path(cluster.getFinalDestDirRoot(), streamName);
  }
}
