package com.inmobi.databus.readers;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2012 - 2014 InMobi
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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;
import java.util.TreeSet;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.databus.mapred.DatabusInputFormat;
import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.messaging.consumer.util.DatabusUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestDatabusEmptyFolders extends TestAbstractDatabusWaitingReader {

  protected ClusterUtil cluster;

  @BeforeTest
  public void setup() throws Exception {
    TestUtil.cleanupCluster(cluster);

    files = new String[]{TestUtil.files[1]};
    consumerNumber = 1;
    // initialize config
    cluster = TestUtil.setupLocalCluster(this.getClass().getSimpleName(),
        testStream, new PartitionId(clusterName, collectorName), files, null,
        finalFiles, files.length, 0, TestUtil.getConfiguredRootDir());
    conf = cluster.getHadoopConf();
    fs = FileSystem.get(conf);
    streamDir = getStreamsDir();
    inputFormatClass = DatabusInputFormat.class.getCanonicalName();
    encoded = true;
    partitionMinList = new TreeSet<Integer>();
    for (int i = 0; i < 60; i++) {
      partitionMinList.add(i);
    }
    chkPoints = new TreeMap<Integer, PartitionCheckpoint>();
    partitionCheckpointList = new PartitionCheckpointList(chkPoints);
  }

  @AfterTest
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  @Override
  Path getStreamsDir() {
    return TestUtil.getStreamsLocalDir(cluster, testStream);
  }

  @Test
  public void testEmptyFolder() throws Exception {
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    PartitionReaderStatsExposer metrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    lreader = new DatabusStreamWaitingReader(partitionId,
        fs, getStreamsDir(), inputFormatClass, conf, 1000, metrics, false,
        partitionMinList, partitionCheckpointList, null);

    createMoreEmptyFolders();
    String lastFolder = removeFilesIfAny().toString();
    String path = lastFolder.substring(lastFolder.indexOf("testclient"));
    path = path.substring(path.indexOf("/") + 1);
    SimpleDateFormat format =
        new SimpleDateFormat("yyyy" + "/" + "MM" + "/" + "dd" + "/" + "HH" + "/" + "mm");
    Date date = modifyTime(format.parse(path), Calendar.MINUTE, -1);

    lreader.build(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        finalFiles[0].getParent()));
    Assert.assertEquals(roundOffSecs(lreader.getBuildTimestamp()), date);

    lreader.build();
    Assert.assertEquals(roundOffSecs(lreader.getBuildTimestamp()), date);

    lreader.build();
    Assert.assertEquals(roundOffSecs(lreader.getBuildTimestamp()), date);
  }

  private void createMoreEmptyFolders() throws IOException {
    Calendar cl = Calendar.getInstance();
    cl.add(Calendar.MINUTE, -25);
    Date startDate = cl.getTime();

    cl = Calendar.getInstance();
    cl.add(Calendar.MINUTE, -5);
    Date endDate = cl.getTime();

    while (startDate.before(endDate)) {
      Path baseDir = DatabusUtil.getStreamDir(StreamType.LOCAL,
          new Path(cluster.getRootDir()), testStream);
      Path minDir = DatabusStreamReader.getMinuteDirPath(baseDir, startDate);
      fs.mkdirs(minDir);
      Calendar instance = Calendar.getInstance();
      instance.setTime(startDate);
      instance.add(Calendar.MINUTE, 1);
      startDate = instance.getTime();
    }
  }

  private Path removeFilesIfAny() throws IOException {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    Path streamDir = DatabusUtil.getStreamDir(StreamType.LOCAL,
        new Path(cluster.getRootDir()), testStream);
    Path minuteDirPath = DatabusStreamReader.getMinuteDirPath(streamDir,
        modifyTime(new Date(), Calendar.MINUTE, -10));
    FileStatus[] fileStatuses = fs.listStatus(minuteDirPath.getParent());
    for (FileStatus folders : fileStatuses) {
      if (!folders.isDir()) {
        continue;
      }
      FileStatus[] files = fs.listStatus(folders.getPath());
      for (FileStatus file : files) {
        if (file.isDir()) {
          continue;
        }
        fs.delete(file.getPath());
      }
    }
    return fileStatuses[fileStatuses.length - 1].getPath();
  }

  private Date modifyTime(Date dt, int field, int unit) {
    Calendar c = Calendar.getInstance();
    c.setTime(dt);
    c.add(field, unit);
    return c.getTime();
  }

  private Date roundOffSecs(Date ts) {
    long time = ts.getTime();
    return new Date(time - (time % 60000));
  }
}
