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
import java.util.HashMap;
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
import org.testng.Assert;

import com.inmobi.databus.files.StreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.partition.PartitionReader;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.EOFMessage;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.HadoopUtil;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public abstract class TestAbstractClusterReader {
  private static final Log LOG = LogFactory.getLog(
      TestAbstractClusterReader.class);
  protected static final String testStream = "testclient";
  protected static final String clusterName = "testCluster";
  protected PartitionId partitionId = new PartitionId(clusterName, null);
  protected LinkedBlockingQueue<QueueEntry> buffer =
      new LinkedBlockingQueue<QueueEntry>(1000);
  protected PartitionReader preader;
  public Set<Integer> partitionMinList;
  PartitionCheckpointList partitionCheckpointList;
  Map<Integer, PartitionCheckpoint> pchkPoints;
  int consumerNumber;

  protected String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  protected Path[] databusFiles = new Path[3];

  protected final String collectorName = "collector1";
  FileSystem fs;
  Path streamDir;
  Configuration conf = new Configuration();
  String inputFormatClass;

  public void cleanup() throws IOException {
    LOG.debug("Cleaning up the dir: " + streamDir.getParent());
    fs.delete(streamDir.getParent(), true);
  }

  abstract Path getStreamsDir();
  abstract boolean isDatabusData();

  public void testInitialize() throws Exception {
    initializeMinList();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    // Read from start
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
        streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, partitionMinList, null);
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[0].toString()));

    // Read from checkpoint with local stream file name
    initializePartitionCheckpointList();
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1],
        partitionCheckpointList);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), prMetrics, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[1].toString()));

    // Read from checkpoint with local stream file name which does not exist
    // and is before the stream
    initializePartitionCheckpointList();
    prepareCheckpoint(HadoopUtil.getOlderFile(streamDir, fs, databusFiles[0]),
        20, databusFiles[1], partitionCheckpointList);

    preader = new PartitionReader(partitionId, partitionCheckpointList,
        fs, buffer, streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), prMetrics, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[0].toString()));

    //Read from startTime in local stream directory, with no checkpoint
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[1].getParent()));

    initializePartitionCheckpointList();
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs, buffer,
        streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[1].toString()));

    //Read from startTime in local stream directory, with checkpoint
    initializePartitionCheckpointList();
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 20, databusFiles[0],
        partitionCheckpointList);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[0].toString()));

    //Read from startTime in local stream directory, with no timestamp file,
    // with no checkpoint
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    cal.add(Calendar.MINUTE, 1);
    initializePartitionCheckpointList();
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[1].toString()));

    //Read from checkpoint in local stream directory, with no timestamp file,
    //with start time
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 20, databusFiles[0],
        partitionCheckpointList);
    preader = new PartitionReader(partitionId,partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[0].toString()));

    //Read from startTime beyond the stream
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    cal.add(Calendar.MINUTE, -2);
    initializePartitionCheckpointList();
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[0].toString()));

    //Read from startTime after the stream
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[2].getParent()));
    cal.add(Calendar.MINUTE, 2);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertNull(preader.getCurrentFile());
  }

  public void testReadFromStart() throws Exception {
    initializeMinList();
    initializePartitionCheckpointList();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    String fsUri = fs.getUri().toString();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass,
        DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
            databusFiles[0].getParent()),
            1000, isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[0]), streamDir, partitionMinList,
        partitionCheckpointList, true, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[0]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromCheckpoint() throws Exception {
    initializeMinList();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 20, databusFiles[1],
        partitionCheckpointList);

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[1]);
    Date toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, true, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 180);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 180);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  /*
   * Test the reader with checkpoint which does not exist and start time.
   * Reader should read from the start of the stream as checkpoint does not
   * exist and ignore the start time because checkpoint has higher precedence
   */
  public void testReadFromCheckpointWhichDoesNotExist() throws Exception {
    initializeMinList();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    prepareCheckpoint(HadoopUtil.getOlderFile(streamDir, fs, databusFiles[0]),
        20, databusFiles[0], partitionCheckpointList);

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass,
        DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
            databusFiles[1].getParent()), 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[0]),streamDir, partitionMinList,
        partitionCheckpointList, true, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[0]),streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromStartTime() throws Exception {
    initializeMinList();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass,
        DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
            databusFiles[1].getParent()),
            1000,
            isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[1]);
    Date toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, true, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 200);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromStartTimeWithinStream() throws Exception {
    initializeMinList();
    initializePartitionCheckpointList();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    cal.add(Calendar.MINUTE, 1);
    String fsUri = fs.getUri().toString();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    Date fromTime = cal.getTime();
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        null, streamDir, partitionMinList, partitionCheckpointList, true, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();

    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 200);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 200);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromStartTimeBeforeStream() throws Exception {
    initializeMinList();
    initializePartitionCheckpointList();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));
    cal.add(Calendar.MINUTE, -1);
    String fsUri = fs.getUri().toString();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.execute();
    Date fromTime = cal.getTime();
    Date toTime = getTimeStampFromFile(databusFiles[0]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        null, streamDir, partitionMinList, partitionCheckpointList, true, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    fromTime = getTimeStampFromFile(databusFiles[0]);
    toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[0]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 00, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromStartTimeAfterStream() throws Exception {
    initializeMinList();
    initializePartitionCheckpointList();
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[2].getParent()));
    cal.add(Calendar.MINUTE, 2);
    String fsUri = fs.getUri().toString();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 0);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 0);
    Assert.assertEquals(prMetrics.getCumulativeNanosForFetchMessage(), 0);
  }

  public void testReadFromCheckpointWithSingleMinute() throws Exception {
    partitionMinList = new TreeSet<Integer>();
    Map<Integer, PartitionCheckpoint> chkpoints = new
        TreeMap<Integer, PartitionCheckpoint>();
    partitionCheckpointList = new PartitionCheckpointList(chkpoints);
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();

    for (int i = 0; i < 1; i++) {
      Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
          databusFiles[i].getParent());
      Calendar current = Calendar.getInstance();
      current.setTime(date);
      partitionMinList.add(current.get(Calendar.MINUTE));
      partitionCheckpointList.set(current.get(Calendar.MINUTE), new
          PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
              fs.getFileStatus(databusFiles[i])), 20));
    }

    String fsUri = fs.getUri().toString();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[0].toString()));

    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 80);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 80);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromCheckpointMultipleMinutes() throws Exception {
    partitionMinList = new TreeSet<Integer>();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();

    for (int i = 0; i < 3; i++) {
      Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
          databusFiles[i].getParent());
      Calendar current = Calendar.getInstance();
      current.setTime(date);
      partitionMinList.add(current.get(Calendar.MINUTE));
      partitionCheckpointList.set(current.get(Calendar.MINUTE), new
          PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
              fs.getFileStatus(databusFiles[i])), 20));
    }

    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), prMetrics, true,partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[0].toString()));

    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[0]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 240);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 240);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0); 
  }

  public void testReadFromCheckpointSomeMinutes()  throws Exception {
    partitionMinList = new TreeSet<Integer>();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    for (int i = 0; i < 3; i++) {
      Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
          databusFiles[i].getParent());
      Calendar current = Calendar.getInstance();
      current.setTime(date);
      partitionMinList.add(current.get(Calendar.MINUTE));
      if (i != 1) {
        partitionCheckpointList.set(current.get(Calendar.MINUTE), new
            PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
                fs.getFileStatus(databusFiles[i])), 20));
      } else {
        partitionCheckpointList.set(current.get(Calendar.MINUTE), new
            PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
                fs.getFileStatus(databusFiles[i])), 00));
      }
    }

    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[1].getParent()));

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(),
        1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[0].toString()));

    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[0]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 260);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 260);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  /*
   * If all the checkpoints are complete checkpoints(i.e all are having line
   * number as -1) then no files will be added to file map. Reader cannot read
   * any file. Number of messages read by reader and number of messages added
   * to the buffer should be zero.
   */
  public void testReadFromMultipleCompleteCheckpoints() throws Exception {
    partitionMinList = new TreeSet<Integer>();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();

    for (int i = 0; i < 3; i++) {
      Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
          databusFiles[i].getParent());
      Calendar current = Calendar.getInstance();
      current.setTime(date);
      partitionMinList.add(current.get(Calendar.MINUTE));
      partitionCheckpointList.set(current.get(Calendar.MINUTE), new
          PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
              fs.getFileStatus(databusFiles[i])), -1));
    }

    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, null, 1000,
        isDatabusData(), prMetrics, true,partitionMinList, null);
    preader.init();
    /*
     * In general, Partition reader has to start reading once it finds some
     * files and exited from init method. If there are no files to read then
     * reader has to wait until the new files are available. But we are using
     * noNewFiles flag is to avoid continuous looping for test cases. Reader does
     *  not wait for the new files creation if we set noNewFiles flag is true..
     */
    if (preader.getCurrentFile() != null) {
      // this code can not be reached as current file always null.
      preader.execute();   
    }
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 0, 0, partitionId,
        buffer, isDatabusData(), null);

    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 0, partitionId,
        buffer, isDatabusData(), null);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 0, partitionId,
        buffer, isDatabusData(), null);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 0);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 0);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
  }

  public void testReadFromSingleCompleteCheckpoint() throws Exception {
    partitionMinList = new TreeSet<Integer>();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();

    for (int i = 0; i < 3; i++) {
      Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
          databusFiles[i].getParent());
      Calendar current = Calendar.getInstance();
      current.setTime(date);
      partitionMinList.add(current.get(Calendar.MINUTE));
      if (i != 0) {
        partitionCheckpointList.set(current.get(Calendar.MINUTE), new
            PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
                fs.getFileStatus(databusFiles[i])), 20));
      } else {
        partitionCheckpointList.set(current.get(Calendar.MINUTE), new
            PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
                fs.getFileStatus(databusFiles[i])), -1));
      }
    }

    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[1].toString()));
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        null, streamDir, partitionMinList, partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 160);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 160);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void testReadFromTwoCompleteCheckpoint() throws Exception {
    partitionMinList = new TreeSet<Integer>();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    for (int i = 0; i < 3; i++) {
      Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
          databusFiles[i].getParent());
      Calendar current = Calendar.getInstance();
      current.setTime(date);
      partitionMinList.add(current.get(Calendar.MINUTE));
      if (i != 2) {
        partitionCheckpointList.set(current.get(Calendar.MINUTE), new
            PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
                fs.getFileStatus(databusFiles[i])), -1));
      }
    }

    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[2].toString()));
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        null, streamDir, partitionMinList, partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 100);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 100);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  /*
   * This test case tests the scenario where some checkpoints exists and some
   * does not exists
   */
  public void testReadFromCheckpointsAndSomeNotExists()  throws Exception {
    partitionMinList = new TreeSet<Integer>();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    for (int i = 0; i < 3; i++) {
      Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
          databusFiles[i].getParent());
      Calendar current = Calendar.getInstance();
      current.setTime(date);
      partitionMinList.add(current.get(Calendar.MINUTE));
      if (i != 1) {
        partitionCheckpointList.set(current.get(Calendar.MINUTE), new
            PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
                fs.getFileStatus(databusFiles[i])), 20));
      } else {
        prepareCheckpoint(HadoopUtil.getOlderFile(streamDir, fs, databusFiles[i]),
            20, databusFiles[i], partitionCheckpointList);
      }
    }
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[0].toString()));

    preader.execute();
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), 1, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[0]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 260);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 260);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }
  
  /*
   * this test is used to test the scenario where some checkpoints exists with
   * line number has -1 and some of them are not exists
   *
   */
  public void testReadFromExistingCompletedCheckpointAndCheckpointNotExists()
      throws Exception {
    partitionMinList = new TreeSet<Integer>();
    initializePartitionCheckpointList();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();

    for (int i = 0; i < 3; i++) {
      Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
          databusFiles[i].getParent());
      Calendar current = Calendar.getInstance();
      current.setTime(date);
      partitionMinList.add(current.get(Calendar.MINUTE));
      if (i == 0) {
        partitionCheckpointList.set(current.get(Calendar.MINUTE), new
            PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
                fs.getFileStatus(databusFiles[i])), -1));

      } else if (i == 1) {
        prepareCheckpoint(HadoopUtil.getOlderFile(streamDir, fs,
            databusFiles[i]), 20, databusFiles[i], partitionCheckpointList);
      } else {
        partitionCheckpointList.set(current.get(Calendar.MINUTE), new
            PartitionCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
                fs.getFileStatus(databusFiles[i])), 20));
      }
    }

    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Calendar cal = Calendar.getInstance();
    cal.setTime(DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFiles[0].getParent()));

    preader = new PartitionReader(partitionId, partitionCheckpointList, fs,
        buffer, streamDir, conf, inputFormatClass, cal.getTime(), 1000,
        isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[1].toString()));
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        null, streamDir, partitionMinList, partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 00, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(databusFiles[1]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 20, 80, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.take().getMessage() instanceof EOFMessage);
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Assert.assertEquals(prMetrics.getHandledExceptions(), 0);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 180);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 180);
    Assert.assertEquals(prMetrics.getWaitTimeUnitsNewFile(), 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  public void prepareCheckpoint(StreamFile streamFile, int lineNum,
      Path databusFile, PartitionCheckpointList partitionCheckpointList) {
    Date date = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
        databusFile.getParent());
    Calendar current = Calendar.getInstance();
    current.setTime(date);
    partitionCheckpointList.set(current.get(Calendar.MINUTE), new
        PartitionCheckpoint(streamFile, lineNum));
  }

  public void initializeMinList() {
    partitionMinList = new TreeSet<Integer>();
    for (int i = 0; i < 60; i++) {
      partitionMinList.add(i);
    }
  }

  public void initializePartitionCheckpointList() {
    pchkPoints = new TreeMap<Integer, PartitionCheckpoint>();
    partitionCheckpointList = new PartitionCheckpointList(pchkPoints);
  }

  public String getDateStringFromPath(String path) {
    String[] str = path.split("[0-9]{4}.[0-9]{2}.[0-9]{2}.[0-9]{2}.[0-9]{2}");
    return path.substring(str[0].length());
  }

  protected Date getTimeStampFromFile(Path dir) {
    return DatabusStreamWaitingReader.getDateFromStreamDir(streamDir, dir);
  }
}
