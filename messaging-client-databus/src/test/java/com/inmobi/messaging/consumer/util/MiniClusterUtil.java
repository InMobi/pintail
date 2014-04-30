package com.inmobi.messaging.consumer.util;

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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class MiniClusterUtil {

  private static MiniDFSCluster dfsCluster;
  private static FileSystem lfs;
  private static int numAccess = 0;

  public static synchronized MiniDFSCluster getDFSCluster(Configuration conf)
      throws IOException {
    if (dfsCluster == null) {
      lfs = FileSystem.getLocal(conf);
      lfs.delete(new Path(MiniClusterUtil.getBaseDirectory().toString()), true);
      dfsCluster = new MiniDFSCluster(conf, 1, true, null);
    }
    numAccess++;
    return dfsCluster;
  }

  public static synchronized void shutdownDFSCluster() throws IOException {
    numAccess--;
    if (numAccess == 0) {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
      lfs.delete(new Path(MiniClusterUtil.getBaseDirectory().toString()), true);
    }
  }
  /**
   * Just for cross version compatibility , should be removed later
   * @return
   */
  private static File getBaseDirectory(){
	  return new File(System.getProperty( "test.build.data", "build/test/data"), "dfs/");
  }
  
  
}
