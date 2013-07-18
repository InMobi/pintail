package com.inmobi.databus.partition;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.databus.QueueEntry;
import com.inmobi.messaging.consumer.util.TestUtil;
import com.inmobi.messaging.metrics.PartitionReaderStatsExposer;

public abstract class TestAbstractWaitingClusterReader {

  protected static final String testStream = "testclient";
  protected static final String clusterName = "testCluster";
  protected PartitionId partitionId = new PartitionId(clusterName, null);

  protected LinkedBlockingQueue<QueueEntry> buffer =
      new LinkedBlockingQueue<QueueEntry>(150);
  protected Cluster cluster;
  protected PartitionReader preader;
  Set<Integer>  partitionMinList;
  PartitionCheckpointList partitionCheckpointlist;


  protected String[] files = new String[] {TestUtil.files[1], TestUtil.files[3],
      TestUtil.files[5]};
  protected String[] newFiles = new String[] {TestUtil.files[6], TestUtil.files[7],
      TestUtil.files[8]};
  protected Path[] databusFiles = new Path[3];

  protected final String collectorName = "collector1";
  FileSystem fs;
  String inputFormatClass;
  Path streamDir;
  Configuration conf;
  int consumerNumber;

  abstract void setupFiles(String[] files, Path[] newDatabusFiles) throws
      Exception;
  abstract boolean isDatabusData();
  
  public void cleanup() throws IOException {
    TestUtil.cleanupCluster(cluster);
  }

  public void testReadFromStart() throws Exception {
    String fsUri = fs.getUri().toString();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, partitionCheckpointlist, fs,
    		buffer, streamDir, conf, inputFormatClass,
        DatabusStreamWaitingReader.getDateFromStreamDir(streamDir,
            databusFiles[0]),
        1000, isDatabusData(), prMetrics, false, partitionMinList,
        null);

    testReader(preader, prMetrics);
  }

  public void testReadFromStartOfStream() throws Exception {
    String fsUri = fs.getUri().toString();
    PartitionReaderStatsExposer prMetrics = new PartitionReaderStatsExposer(
        testStream, "c1", partitionId.toString(), consumerNumber, fsUri);
    preader = new PartitionReader(partitionId, partitionCheckpointlist, fs,
        buffer, streamDir, conf, inputFormatClass, null,
        1000, isDatabusData(), prMetrics, false, partitionMinList, null);

    testReader(preader, prMetrics);
  }

  private void testReader(PartitionReader preader,
      PartitionReaderStatsExposer prMetrics) throws Exception {
    Map<Integer, PartitionCheckpoint> expectedDeltaPck = new HashMap<Integer,
        PartitionCheckpoint>();
    preader.init();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertEquals(preader.getReader().getClass().getName(),
        ClusterReader.class.getName());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    preader.start();
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    FileStatus fs0 = fs.getFileStatus(databusFiles[0]);
    FileStatus fs1 = fs.getFileStatus(databusFiles[1]);
    fs.delete(databusFiles[0], true);
    fs.delete(databusFiles[1], true);
    fs.delete(databusFiles[2], true);
    Path[] newDatabusFiles = new Path[3];
    setupFiles(new String[] {newFiles[0]}, newDatabusFiles);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs0), 1, 0, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPck);
    Date fromTime = getTimeStampFromFile(databusFiles[0]);
    Date toTime = getTimeStampFromFile(databusFiles[1]);
    prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck, fs0);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs1), 2, 0, 50, partitionId, buffer,
        isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    while (buffer.remainingCapacity() > 0) {
      Thread.sleep(10);
    }
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs1), 2, 50, 50, partitionId, buffer, isDatabusData(), expectedDeltaPck);
    fromTime = getTimeStampFromFile(databusFiles[1]);
    toTime = getTimeStampFromFile(newDatabusFiles[0]);
    prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck, fs1);
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(newDatabusFiles[0])), 1, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();
    Assert.assertTrue(buffer.isEmpty());
    Assert.assertNotNull(preader.getReader());
    Assert.assertEquals(((ClusterReader) preader.getReader())
        .getReader().getClass().getName(),
        DatabusStreamWaitingReader.class.getName());
    Thread.sleep(1000);
    Path previousFile = newDatabusFiles[0];
    setupFiles(new String[] {newFiles[1], newFiles[2]},
        newDatabusFiles);

    fromTime = getTimeStampFromFile(previousFile);
    toTime = getTimeStampFromFile(newDatabusFiles[0]);
    prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(previousFile));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(newDatabusFiles[0])), 1, 0, 100, partitionId, buffer,
        isDatabusData(), expectedDeltaPck);
    expectedDeltaPck.clear();

    fromTime = getTimeStampFromFile(newDatabusFiles[0]);
    toTime = getTimeStampFromFile(newDatabusFiles[1]);
    prepareExpectedDeltaPck(fromTime, toTime, expectedDeltaPck,
        fs.getFileStatus(newDatabusFiles[0]));
    TestUtil.assertBuffer(DatabusStreamWaitingReader.getHadoopStreamFile(
        fs.getFileStatus(newDatabusFiles[1])), 2, 0, 100, partitionId,
        buffer, isDatabusData(), expectedDeltaPck);
    Assert.assertTrue(buffer.isEmpty());
    preader.close();
    Assert.assertEquals(prMetrics.getMessagesReadFromSource(), 500);
    Assert.assertEquals(prMetrics.getMessagesAddedToBuffer(), 500);
    Assert.assertTrue(prMetrics.getWaitTimeUnitsNewFile() > 0);
    Assert.assertTrue(prMetrics.getCumulativeNanosForFetchMessage() > 0);
  }

  private void prepareExpectedDeltaPck(Date fromTime, Date toTime,
      Map<Integer, PartitionCheckpoint> expectedDeltaPck, FileStatus file) {
    Map<Integer, Date> chkTimeStampMap = new HashMap<Integer, Date>();
    // prepare a checkpoint map
    prepareChkpointTimeMap(chkTimeStampMap);
    Calendar current = Calendar.getInstance();
    current.setTime(fromTime);
    if (file != null) {
      int minute = current.get(Calendar.MINUTE);
      expectedDeltaPck.put(Integer.valueOf(minute), new PartitionCheckpoint(
          DatabusStreamWaitingReader.getHadoopStreamFile(file), -1));
      current.add(Calendar.MINUTE, 1);
    }
    while (current.getTime().before(toTime)) {
      int minute = current.get(Calendar.MINUTE);
      if (partitionMinList.contains(minute)) {
        Date chkTime = chkTimeStampMap.get(minute);
        if (chkTime == null || chkTime.before(current.getTime())) {
          expectedDeltaPck.put(Integer.valueOf(minute),
              new PartitionCheckpoint(DatabusStreamWaitingReader.
                  getHadoopStreamFile(streamDir, current.getTime()), -1));
        }
      }
      current.add(Calendar.MINUTE, 1);
    }
  }

  private void prepareChkpointTimeMap(Map<Integer, Date> chkTimeStampMap) {
    Map<Integer, PartitionCheckpoint> partitionChkList =
        partitionCheckpointlist.getCheckpoints();
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

  private Date getTimeStampFromFile(Path dir) {
    return DatabusStreamWaitingReader.getDateFromStreamDir(streamDir, dir);
  }
}
