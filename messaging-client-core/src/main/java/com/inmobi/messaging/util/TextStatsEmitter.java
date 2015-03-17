package com.inmobi.messaging.util;

/*
 * #%L
 * messaging-client-core
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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import com.inmobi.stats.StatsExposer;

public class TextStatsEmitter extends RunnableStatsEmitter {

  private String statsPath;
  private boolean appendStats = true;

  @Override
  public void init(Properties props) {
    sleep = Integer.valueOf(props.getProperty("text.emitter.poll.interval",
        "10000"));
    statsPath = props.getProperty("text.emitter.statspath", ".");
    appendStats = Boolean.parseBoolean(props.getProperty(
        "text.emitter.append.stats", "true"));
    createThread();
  }

  protected void writeStats() {
    Map<String, Number> stats;
    Map<String, String> contexts;
    try {
      synchronized (statsExposers) {
        for (StatsExposer exposer : statsExposers) {
          stats = exposer.getStats();
          contexts = exposer.getContexts();

          String statsFileName = statsPath + "/context";
          for (Map.Entry<String, String> pair: contexts.entrySet()) {
            statsFileName += "-" + pair.getKey() + "_" + pair.getValue();
          }
          statsFileName += ".txt";

          FileWriter fw = new FileWriter(statsFileName, appendStats);
          BufferedWriter out = new BufferedWriter(fw);

          out.write(new Date().toString());
          out.write("\n");
          for (Map.Entry<String, Number> stat: stats.entrySet()) {
            out.write(stat.getKey());
            out.write(":");
            out.write(stat.getValue().toString());
            out.write("\n");
          }
          out.close();
        }
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

}
