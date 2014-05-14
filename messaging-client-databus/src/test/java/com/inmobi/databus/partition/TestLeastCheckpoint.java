package com.inmobi.databus.partition;

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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.consumer.util.TestUtil;

public class TestLeastCheckpoint {
  
  FileSystem fs;
  Map<Integer, PartitionCheckpoint> chkPoints;
  PartitionCheckpoint expectedLeastPck;
  protected Path rootDir;

  public TestLeastCheckpoint() {

  }

  @BeforeTest
  public void setup() throws Exception {
    fs =  FileSystem.getLocal(new Configuration());
    rootDir = new Path(TestUtil.getConfiguredRootDir(),
        this.getClass().getSimpleName());
    chkPoints = new HashMap<Integer, PartitionCheckpoint>();
    createCheckpointList();
  }

  public HadoopStreamFile createPaths(Path p1, int minute) throws Exception {
    fs.mkdirs(p1);
    Path pf11 = new Path(p1, "f1");
    fs.create(pf11);
    FileStatus fs11 = fs.getFileStatus(pf11);
    return HadoopStreamFile.create(fs11);
  }

  private void createCheckpointList() throws Exception {
    Path p1 = new Path(rootDir, "2012/12/26/05/00");
    Path p2 = new Path(rootDir, "2012/12/26/02/01");
    Path p3 = new Path(rootDir, "2012/12/26/03/02");
    Path p4 = new Path(rootDir, "2012/12/26/01/03");
    Path p5 = new Path(rootDir, "2012/12/26/02/04");
    HadoopStreamFile streamfile1 = createPaths(p1, 0);
    HadoopStreamFile streamfile2 = createPaths(p2, 1);
    HadoopStreamFile streamfile3 = createPaths(p3, 2);
    HadoopStreamFile streamfile4 = createPaths(p4, 3);
    HadoopStreamFile streamfile5 = createPaths(p5, 4);

    chkPoints.put(00, null);
    chkPoints.put(01, new PartitionCheckpoint(streamfile2, 100));
    chkPoints.put(02, new PartitionCheckpoint(streamfile3, -1));
    chkPoints.put(3, null);
    chkPoints.put(04, new PartitionCheckpoint(streamfile4, 0));
    chkPoints.put(05, new PartitionCheckpoint(streamfile5, 100));

    expectedLeastPck = new PartitionCheckpoint(streamfile4, 0);
  }

  @Test
  public void testLeastCheckpoint() throws Exception {
    DatabusStreamWaitingReader reader = new DatabusStreamWaitingReader(null, fs,
        null, TextInputFormat.class.getCanonicalName(), new Configuration(), 0L,
        null, false, chkPoints.keySet(),
        new PartitionCheckpointList(chkPoints), null);
    PartitionCheckpoint leastPartitionCheckpoint = reader.getLeastCheckpoint();
    System.out.println("least value " + leastPartitionCheckpoint);
    Assert.assertEquals(leastPartitionCheckpoint, expectedLeastPck);
  }

  @AfterTest
  public void cleanup() throws IOException {
    fs.delete(rootDir, true);
  }
}
