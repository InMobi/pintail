package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.HadoopUtil;

import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class TestPartitionReaderWithLeastFullCheckpoint extends TestAbstractClusterReader{

  static final Log LOG = LogFactory.getLog(TestPartitionReaderWithLeastFullCheckpoint.class);

  @BeforeMethod
  public void setup() throws Exception {
    consumerNumber = 1;
    files = new String[] {HadoopUtil.files[1], HadoopUtil.files[3],
        HadoopUtil.files[5]};
    databusFiles = new Path[6];
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    streamDir = new Path(new Path(TestUtil.getConfiguredRootDir(),
        this.getClass().getSimpleName()), testStream).makeQualified(fs);
    // initialize config
    HadoopUtil.setupHadoopCluster(conf, files, null, databusFiles, streamDir,
        true);
    inputFormatClass = SequenceFileInputFormat.class.getCanonicalName();
    partitionMinList = new HashSet<Integer>();
    for (int i = 0; i < 60; i++) {
      partitionMinList.add(i);
    }
    pchkPoints = new TreeMap<Integer, PartitionCheckpoint>();
    partitionCheckpointList = new PartitionCheckpointList(pchkPoints);
  }

  @AfterMethod
  public void cleanup() throws IOException {
    LOG.debug("Cleaning up the dir: " + streamDir.getParent());
    fs.delete(streamDir.getParent(), true);
  }

  @Test
  public void testReadFromLeastFullCheckpoint() throws Exception {
    buffer.clear();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPchk = new HashMap<Integer,
        PartitionCheckpoint>();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    fs.delete(databusFiles[1], true);
    fs.mkdirs(databusFiles[1].getParent());
    fs.delete(databusFiles[2], true);
    fs.mkdirs(databusFiles[2].getParent());
    prepareCheckpoint(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[0])), -1, databusFiles[0],
        partitionCheckpointList);
    PartitionReader preader = new PartitionReader(partitionId,
        partitionCheckpointList, fs, buffer, streamDir, conf, inputFormatClass,
        null, 1000, isDatabusData(), prMetrics, true, partitionMinList, null);
    preader.init();
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[3].toString()));
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[3]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPchk, null,
        streamDir, partitionMinList, partitionCheckpointList, true, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[3])), 4, 00, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPchk);
    expectedDeltaPchk.clear();
    fromTime = getTimeStampFromFile(databusFiles[3]);
    toTime = getTimeStampFromFile(databusFiles[4]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPchk,
        fs.getFileStatus(databusFiles[3]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[4])), 5, 00, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPchk);
    expectedDeltaPchk.clear();
    fromTime = getTimeStampFromFile(databusFiles[4]);
    toTime = getTimeStampFromFile(databusFiles[5]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPchk,
        fs.getFileStatus(databusFiles[4]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[5])), 6, 00, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPchk);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);
    Assert.assertEquals(prMetrics.getCurrentMinuteBeingRead(),
        DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
            databusFiles[5]).getTime());
  }

  @Test
  public void testDeltaCheckpointWithStoptime() throws Exception {
    buffer.clear();
    String fsUri = fs.getUri().toString();
    Map<Integer, PartitionCheckpoint> expectedDeltaPchk = new HashMap<Integer,
        PartitionCheckpoint>();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    Date startTime = DatabusStreamWaitingReader.getDateFromStreamDir(streamDir, databusFiles[1]);
    Calendar cal = Calendar.getInstance();
    cal.setTime(startTime);
    cal.add(Calendar.HOUR_OF_DAY, 1);
    cal.add(Calendar.MINUTE, -5);
    Date stopTime = cal.getTime();
    PartitionReader preader = new PartitionReader(partitionId,
        partitionCheckpointList, fs, buffer, streamDir, conf, inputFormatClass,
        startTime, 1000, isDatabusData(), prMetrics, false, partitionMinList, stopTime);
    preader.init();
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[1]);
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPchk, null,
        streamDir, partitionMinList, partitionCheckpointList, true, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[1])), 2, 00, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPchk);
    expectedDeltaPchk.clear();
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(databusFiles[2]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPchk,
        fs.getFileStatus(databusFiles[1]),
        streamDir, partitionMinList, partitionCheckpointList, true, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[2])), 3, 00, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPchk);
    expectedDeltaPchk.clear();
    fromTime = getTimeStampFromFile(databusFiles[2]);
    cal.add(Calendar.MINUTE, 1);
    toTime = cal.getTime();
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPchk,
        fs.getFileStatus(databusFiles[2]), streamDir, partitionMinList,
        partitionCheckpointList, false, true);
    QueueEntry entry = buffer.take();
    Assert.assertEquals(((DeltaPartitionCheckPoint)entry.getMessageChkpoint()).
        getDeltaCheckpoint(), expectedDeltaPchk);
    Assert.assertEquals(prMetrics.getCurrentMinuteBeingRead(), stopTime.getTime());
  }

  @Override
  Path getStreamsDir() {
    return streamDir;
  }

  @Override
  boolean isDatabusData() {
    return false;
  }
}
