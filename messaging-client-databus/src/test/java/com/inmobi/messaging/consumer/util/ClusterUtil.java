/*
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
 */
package com.inmobi.messaging.consumer.util;

import java.io.File;
import java.text.ParseException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ClusterUtil {
  @Override
  public String toString() {
    return clustername;
  }

  private final String rootDir;
  private final String hdfsUrl;
  private final String clustername;
  private final String clusterjobqueuename;
  private final Set<String> sourceStreams;
  private final Configuration hadoopConf;
  
  public ClusterUtil(Map<String, String> clusterElementsMap, String rootDir,
      Set<String> sourceStreams) throws Exception {
    this.rootDir = rootDir;
    this.clustername = clusterElementsMap.get("name");
    if (clustername == null)
      throw new ParseException(
          "Cluster Name element not found in cluster configuration", 0);
    this.hdfsUrl = clusterElementsMap.get("hdfsurl");
    if (hdfsUrl == null)
      throw new ParseException(
          "hdfsurl element not found in cluster configuration " + clustername,
          0);
    this.clusterjobqueuename = clusterElementsMap
        .get("jobqueue");
    if (clusterjobqueuename == null)
      throw new ParseException(
          "Cluster jobqueuename element not found in cluster configuration "
              + clustername, 0);
    if (clusterElementsMap.get("jturl") == null)
      throw new ParseException(
          "jturl element not found in cluster configuration " + clustername, 0);
    this.hadoopConf = new Configuration();
    this.hadoopConf.set("mapred.job.tracker",
        clusterElementsMap.get("jturl"));
    this.sourceStreams = sourceStreams;
    this.hadoopConf.set("fs.default.name", hdfsUrl);
    this.hadoopConf.set("mapred.job.queue.name",
    		clusterjobqueuename);
  }

  public String getRootDir() {
    return hdfsUrl + File.separator + rootDir + File.separator;
  }

  public String getLocalFinalDestDirRoot() {
    String dest = hdfsUrl + File.separator + rootDir + File.separator
        + "streams_local" + File.separator;
    return dest;
  }

  public String getHdfsUrl() {
    return hdfsUrl;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public String getName() {
    return clustername;
  }

  public String getFinalDestDirRoot() {
    String dest = hdfsUrl + File.separator + rootDir + File.separator
        + "streams" + File.separator;
    return dest;
  }

  public Path getDataDir() {
    return new Path(hdfsUrl + File.separator + rootDir + File.separator
        + "data");
  }
}

