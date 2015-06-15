package com.inmobi.messaging.consumer.databus;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.util.ClusterUtil;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public abstract class TestAbstractDatabusConsumer {
  static final Log LOG = LogFactory.getLog(TestAbstractDatabusConsumer.class);

  protected static final String COLLECTOR_PREFIX = "collector";
  int numMessagesPerFile = 100;
  int numDataFiles = 3;
  DatabusConsumer testConsumer;
  static final String testStream = "testclient";
  protected String[] collectors;
  protected String[] dataFiles;
  protected String consumerName;
  Path[] rootDirs;
  protected final String relativeStartTime = "30";
  Configuration conf;
  protected String ck1;
  protected String ck2;
  protected String ck3;
  protected String ck4;
  protected String ck5;
  protected String ck6;
  protected String ck7;
  protected String ck8;
  protected String ck9;
  protected String ck10;
  protected String ck11;
  protected String ck12;
  protected String ck13;
  protected String chkpointPathPrefix;

  public void setup(int numFileToMove) throws Exception {

    ClientConfig config = loadConfig();
    config.set(DatabusConsumerConfig.hadoopConfigFileKey, "hadoop-conf.xml");
    testConsumer = getConsumerInstance();
    //System.out.println(testConsumer.getClass().getCanonicalName());
    testConsumer.initializeConfig(config);
    conf = testConsumer.getHadoopConf();
    Assert.assertEquals(conf.get("myhadoop.property"), "myvalue");
    // setup stream, collector dirs and data files
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add(testStream);
    chkpointPathPrefix = config.getString(
        DatabusConsumerConfig.checkpointDirConfig);
    setUpCheckpointPaths();
    rootDirs = testConsumer.getRootDirs();
    for (int i = 0; i < rootDirs.length; i++) {
      Map<String, String> clusterConf = new HashMap<String, String>();
      FileSystem fs = rootDirs[i].getFileSystem(conf);
      clusterConf.put("hdfsurl", fs.getUri().toString());
      clusterConf.put("jturl", "local");
      clusterConf.put("name", "databusCluster" + i);
      clusterConf.put("jobqueue", "default");

      String rootDir = rootDirs[i].toUri().toString();
      if (rootDirs[i].toString().startsWith("file:")) {
        String[] rootDirSplit = rootDirs[i].toString().split("file:");
        rootDir = rootDirSplit[1];
      }
      ClusterUtil cluster = new ClusterUtil(clusterConf, rootDir, sourceNames);
      fs.delete(new Path(cluster.getRootDir()), true);
      Path streamDir = new Path(cluster.getDataDir(), testStream);
      fs.delete(streamDir, true);
      fs.mkdirs(streamDir);
      // Create a dir with COLLECTOR_PREFIX. This will make sure consumer is
      // started with another partition reader thread. This reader thread
      // should not read any files from local stream as we are creating
      // files only for actual collectors. This thread should simply wait
      // for new files(i.e. Switches between local and collector streams).
      fs.mkdirs(new Path(streamDir, COLLECTOR_PREFIX));
      for (String collector : collectors) {
        Path collectorDir = new Path(streamDir, collector);
        fs.delete(collectorDir, true);
        fs.mkdirs(collectorDir);
        TestUtil.setUpFiles(cluster, collector, dataFiles, null, null,
            numFileToMove, numFileToMove);
      }
    }
  }

  private void setUpCheckpointPaths() {
    ck1 = new Path(chkpointPathPrefix, "checkpoint1").toString();
    ck2 = new Path(chkpointPathPrefix, "checkpoint2").toString();
    ck3 = new Path(chkpointPathPrefix, "checkpoint3").toString();
    ck4 = new Path(chkpointPathPrefix, "checkpoint4").toString();
    ck5 = new Path(chkpointPathPrefix, "checkpoint5").toString();
    ck6 = new Path(chkpointPathPrefix, "checkpoint6").toString();
    ck7 = new Path(chkpointPathPrefix, "checkpoint7").toString();
    ck8 = new Path(chkpointPathPrefix, "checkpoint8").toString();
    ck9 = new Path(chkpointPathPrefix, "checkpoint9").toString();
    ck10 = new Path(chkpointPathPrefix, "checkpoint10").toString();
    ck11 = new Path(chkpointPathPrefix, "checkpoint11").toString();
    ck12 = new Path(chkpointPathPrefix, "checkpoint12").toString();
    ck13 = new Path(chkpointPathPrefix, "checkpoint13").toString();
  }

  protected DatabusConsumer getConsumerInstance() {
    return new DatabusConsumer();
  }

  abstract ClientConfig loadConfig();

  void assertMessages(
      ClientConfig config, int numClusters, int numCollectors)
          throws Exception {
    ConsumerUtil.assertMessages(config, testStream, consumerName, numClusters,
        numCollectors,
        numDataFiles, 100, false);
  }

  public void cleanup() throws IOException {
    testConsumer.close();
    for (Path p : rootDirs) {
      FileSystem fs = p.getFileSystem(conf);
      LOG.debug("Cleaning up the dir: " + p);
      fs.delete(p, true);
    }
    FileSystem lfs = new Path(chkpointPathPrefix).getFileSystem(conf);
    lfs.delete(new Path(chkpointPathPrefix).getParent(), true);
  }

}
