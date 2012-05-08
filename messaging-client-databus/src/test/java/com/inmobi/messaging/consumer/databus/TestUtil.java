package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.utils.FileUtil;

public class TestUtil {

  static String constructMessage(int index) {
    StringBuffer str = new StringBuffer();
    str.append(index).append("Message");
    return str.toString();
  }

  static void createEmptyFile(FileSystem fs, Path parent, String fileName)
      throws IOException {
    FSDataOutputStream out = fs.create(new Path(parent, fileName));
    out.close();
  }

  static void createMessageFile(String fileName, FileSystem fs, Path parent,
      int msgIndex) throws IOException {
    FSDataOutputStream out = fs.create(new Path(parent, fileName));
    for (int i = 0; i < 100; i++) {
      out.write(Base64.encodeBase64(constructMessage(msgIndex).getBytes()));
      out.write('\n');
      msgIndex++;
    }
    out.close();
  }

  static void moveFileToStreamLocal(FileSystem fs, String streamName,
      String collectorName, Cluster cluster, Path collectorDir,
      String collectorfileName)
      throws Exception {
    String localStreamFileName = LocalStreamReader.getLocalStreamFileName(
        collectorName, collectorfileName);
    Path streamLocalDateDir = CollectorStreamReader.getDateDir(cluster,
        streamName, collectorfileName);
    Path targetFile = new Path(streamLocalDateDir, localStreamFileName);
    Path collectorPath = new Path(collectorDir, collectorfileName);
    FileUtil.gzip(collectorPath, targetFile, cluster.getHadoopConf());
    fs.delete(collectorPath, true);
  }

  static void assertBuffer(String fileName, int fileNum, int startIndex,
      int numMsgs, PartitionId pid, LinkedBlockingQueue<QueueEntry> buffer)
      throws InterruptedException {
    int fileIndex = (fileNum - 1) * 100 ;
    for (int i = startIndex; i < (startIndex + numMsgs); i++) {
      QueueEntry entry = buffer.take();
      Assert.assertEquals(entry.partitionId, pid);
      Assert.assertEquals(entry.partitionChkpoint,
          new PartitionCheckpoint(fileName, i + 1));
      Assert.assertEquals(new String(entry.message.getData().array()),
          constructMessage(fileIndex + i));
    }
  }

  static void setUpCollectorDataFiles(FileSystem fs, Path collectorDir,
      String... files) throws IOException {
    int i = 0;
    for (String file : files) {
      createMessageFile(file, fs, collectorDir, i);
      i += 100;
    }
  }
  static void setUpEmptyFiles(FileSystem fs, Path collectorDir,
      String... files) throws IOException {
    for (String file : files) {
      createEmptyFile(fs, collectorDir, file);
    }
  }

  static Cluster setupLocalCluster(String className, String testStream,
      PartitionId pid, String[] collectorFiles,
      String[] emptyFiles, int numFilesToMoveToStreamLocal) throws Exception {
    return setupCluster(className, testStream, pid, "file:///", collectorFiles,
        emptyFiles, numFilesToMoveToStreamLocal);
  }

  private static Cluster setupCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles, 
      String[] emptyFiles, int numFilesToMoveToStreamLocal) throws Exception {
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add(testStream);
    Cluster cluster = new Cluster(pid.getCluster(), 
        "/tmp/databus/" + className,
        hdfsUrl, "local", null, sourceNames);
    Path streamDir = new Path(cluster.getDataDir(), testStream);
      
    // setup stream and collector dirs
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    Path collectorDir = new Path(streamDir, pid.getCollector());
    fs.delete(collectorDir, true);
    fs.mkdirs(collectorDir);

    // setup data dirs
    if (collectorFiles != null) {
      TestUtil.setUpCollectorDataFiles(fs, collectorDir, collectorFiles);
    }
    
    if (emptyFiles != null) {
      TestUtil.setUpEmptyFiles(fs, collectorDir, emptyFiles);
    }

    if (numFilesToMoveToStreamLocal > 0 && collectorFiles != null) {
      fs.delete(new Path(cluster.getLocalFinalDestDirRoot()), true);
      for (int i = 0; i < numFilesToMoveToStreamLocal; i++) {
        TestUtil.moveFileToStreamLocal(fs, testStream, pid.getCollector(),
            cluster, collectorDir, collectorFiles[i]);
      }
    }
    return cluster;
  }

  static Cluster setupDFSCluster(String className, String testStream,
      PartitionId pid, String hdfsUrl, String[] collectorFiles,
      String[] emptyFiles, int numFilesToMoveToStreamLocal) throws Exception {
    return setupCluster(className, testStream, pid, hdfsUrl, collectorFiles,
        emptyFiles, numFilesToMoveToStreamLocal);
  }

  static void cleanupCluster(Cluster cluster) throws IOException {
    FileSystem fs = FileSystem.get(cluster.getHadoopConf());
    fs.delete(new Path(cluster.getRootDir()), true);    
  }
  
  static Path getLocalStreamPath(Cluster cluster, String testStream,
      String collectorName, String collectorFile) throws Exception {
    return new Path(CollectorStreamReader.getDateDir(cluster, testStream, collectorFile),
        LocalStreamReader.getLocalStreamFileName(collectorName, collectorFile));
  }
}
