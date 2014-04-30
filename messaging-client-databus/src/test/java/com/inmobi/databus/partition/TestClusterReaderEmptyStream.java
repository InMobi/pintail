package com.inmobi.databus.partition;

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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.readers.CollectorStreamReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class TestClusterReaderEmptyStream {
  static final Log LOG = LogFactory.getLog(TestClusterReaderEmptyStream.class);
  private static final String testStream = "testclient";

  private LinkedBlockingQueue<QueueEntry> buffer =
      new LinkedBlockingQueue<QueueEntry>(1000);
  private PartitionReader preader;
  private static final String clusterName = "testCluster";
  private PartitionId clusterId = new PartitionId(clusterName, null);
  Set<Integer> partitionMinList;
  PartitionCheckpointList partitionCheckpointList;
  Map<Integer, PartitionCheckpoint> chkPoints;

  FileSystem fs;
  Path streamDir;
  Configuration conf = new Configuration();
  String inputFormatClass;
  int consumerNumber;
  String fsUri;

  @BeforeTest
  public void setup() throws Exception {
    // setup cluster
    consumerNumber = 1;
    fs = FileSystem.getLocal(conf);
    fsUri = fs.getUri().toString();
    streamDir = new Path(new Path(TestUtil.getConfiguredRootDir(),
        this.getClass().getSimpleName()),testStream).makeQualified(fs);
    HadoopUtil.setupHadoopCluster(conf, null, null, null, streamDir, false);
    inputFormatClass = SequenceFileInputFormat.class.getName();
    partitionMinList = new TreeSet<Integer>();
    for (int i = 0; i < 60; i++) {
      partitionMinList.add(i);
    }
    chkPoints = new TreeMap<Integer, PartitionCheckpoint>();
    partitionCheckpointList = new PartitionCheckpointList(chkPoints);
  }

  @AfterTest
  public void cleanup() throws IOException {
    LOG.debug("Cleaning up the dir: " + streamDir.getParent());
    fs.delete(streamDir.getParent(), true);
  }

  @Test
  public void testInitialize() throws Exception {
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", clusterId.toString(), consumerNumber, fsUri);
    // Read from start time
    preader = new PartitionReader(clusterId, partitionCheckpointList, fs, buffer,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[0]),
        1000,
        false, prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((AbstractPartitionStreamReader) preader
        .getReader()).getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());

    //Read from checkpoint
    prepareCheckpointList(new HadoopStreamFile(DatabusStreamWaitingReader.
        getMinuteDirPath(streamDir, CollectorStreamReader.
            getDateFromCollectorFile(TestUtil.files[0])),
            "dummyfile", 0L), 20, partitionCheckpointList);
    preader = new PartitionReader(clusterId, partitionCheckpointList, fs, buffer,
        streamDir, conf, inputFormatClass, null,
        1000, false, prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((AbstractPartitionStreamReader) preader
        .getReader()).getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());

    //Read from startTime with checkpoint
    prepareCheckpointList(new HadoopStreamFile(DatabusStreamWaitingReader.
        getMinuteDirPath(streamDir, CollectorStreamReader.
            getDateFromCollectorFile(TestUtil.files[0])),
            "dummyfile", 0L), 20, partitionCheckpointList);
    preader = new PartitionReader(clusterId, partitionCheckpointList, fs, buffer,
        streamDir, conf, inputFormatClass,
        CollectorStreamReader.getDateFromCollectorFile(TestUtil.files[0]),
        1000,
        false, prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((AbstractPartitionStreamReader) preader
        .getReader()).getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
  }

  public void prepareCheckpointList(StreamFile streamFile, int lineNum,
      PartitionCheckpointList partitionCheckpointList) {
    partitionCheckpointList = new PartitionCheckpointList(chkPoints);
    Date date = DatabusStreamWaitingReader.getDateFromCheckpointPath(
        streamFile.toString());
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    partitionCheckpointList.set(cal.get(Calendar.MINUTE),
        new PartitionCheckpoint(streamFile, lineNum));
  }
}
