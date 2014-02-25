package com.inmobi.messaging.consumer.databus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.testng.Assert;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.messaging.consumer.util.MessageUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestAbstractInputFormat {
  private static final Log LOG =
      LogFactory.getLog(TestAbstractInputFormat.class.getName());

  public List<Message> readMessages = new ArrayList<Message>();
  public JobConf defaultConf;
  protected static TaskAttemptContext context;
  public static final Reporter reporter = Reporter.NULL;
  protected static final String testStream = "testclient";
  public String collectors = "collector1";
  protected String[] dataFiles;
  protected Path rootDir;
  protected Configuration conf = new Configuration();
  protected FileSystem fs;
  protected Path collectorDir = null;
  protected ClusterUtil cluster;
  protected TaskAttemptID taskId;

  public void setUp() throws Exception {
    readMessages = new ArrayList<Message>();
    fs = rootDir.getFileSystem(conf);
    defaultConf = new JobConf(new Configuration());
    dataFiles = new String[] {TestUtil.files[0], TestUtil.files[1] };
    setUpCluster();
    fs.delete(new Path(cluster.getRootDir()), true);
    Path streamDir = new Path(cluster.getDataDir(), testStream);
    fs.delete(streamDir, true);
    fs.mkdirs(streamDir);
    collectorDir = new Path(streamDir, collectors);
    fs.delete(collectorDir, true);
    fs.mkdirs(collectorDir);
    TestUtil.setUpFiles(cluster, collectors, dataFiles, null, null,
        1, 0);
  }

  protected void setUpCluster() throws Exception {
    Set<String> sourceNames = new HashSet<String>();
    Map<String, String> clusterConf = new HashMap<String, String>();
    sourceNames.add(testStream);
    clusterConf.put("hdfsurl", fs.getUri().toString());
    clusterConf.put("jturl", "local");
    clusterConf.put("name", "databusCluster" + 0);
    clusterConf.put("jobqueue", "default");
    String rootDirSuffix = null;
    if (rootDir.toString().startsWith("file:")) {
      String[] rootDirSplit = rootDir.toString().split("file:");
      rootDirSuffix = rootDirSplit[1];
    }
    cluster = new ClusterUtil(clusterConf, rootDirSuffix, sourceNames);
  }

  /**
   * @param index construct the messages from the given index.
   */
  public void assertMessages(int index) {
    for (Message msg : readMessages) {
      Assert.assertEquals(new String(msg.getData().array()),
          MessageUtil.constructMessage(index++));
    }
    Assert.assertEquals(readMessages.size(), 100);
  }

  /*
   * lists all the files for the given path
   */
  protected void listAllPaths(Path filePath, List<Path> listOfFiles)
      throws Exception {
    FileStatus [] fileStatuses = fs.listStatus(filePath);
    if (fileStatuses == null || fileStatuses.length == 0) {
      // no files in directory
    } else {
      for (FileStatus file : fileStatuses) {
        if (file.isDir()) {
          listAllPaths(file.getPath(), listOfFiles);
        } else {
          listOfFiles.add(file.getPath());
        }
      }
    }
  }
}
