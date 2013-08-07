package com.inmobi.databus.partition;

import java.io.IOException;
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
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.util.HadoopUtil;

import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public class TestPartitionReaderWithLeastFullCheckpoint extends TestAbstractClusterReader{

  static final Log LOG = LogFactory.getLog(TestPartitionReaderWithLeastFullCheckpoint.class);

  @BeforeTest
  public void setup() throws Exception {
    super.setup();
    consumerNumber = 1;
    files = new String[] {HadoopUtil.files[1], HadoopUtil.files[3],
        HadoopUtil.files[5]};
    databusFiles = new Path[6];
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    streamDir = new Path(new Path(testRootDir, this.getClass().getSimpleName()),
        testStream).makeQualified(fs);
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

  @AfterTest
  public void cleanup() throws IOException {
    LOG.debug("Cleaning up the dir: " + streamDir.getParent());
    fs.delete(streamDir.getParent(), true);
  }

  @Test
  public void testReadFromLeastFullCheckpoint() throws Exception {
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
 /*   Thread.sleep(120000);
    Assert.assertEquals(preader.getCurrentFile().toString(),
        getDateStringFromPath(databusFiles[3].toString()));*/
    preader.execute();
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[3]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPchk, null,
        streamDir, partitionMinList, partitionCheckpointList, true, true);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[3])), 1, 00, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPchk);
    expectedDeltaPchk.clear();
    fromTime = getTimeStampFromFile(databusFiles[3]);
    toTime = getTimeStampFromFile(databusFiles[4]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPchk,
        fs.getFileStatus(databusFiles[3]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[4])), 2, 00, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPchk);
    expectedDeltaPchk.clear();
    fromTime = getTimeStampFromFile(databusFiles[4]);
    toTime = getTimeStampFromFile(databusFiles[5]);
    TestUtil.prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPchk,
        fs.getFileStatus(databusFiles[4]), streamDir, partitionMinList,
        partitionCheckpointList, false, false);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(databusFiles[5])), 3, 00, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPchk);
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 300);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 300);

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
