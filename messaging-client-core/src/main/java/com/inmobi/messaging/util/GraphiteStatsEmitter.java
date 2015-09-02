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

import com.inmobi.messaging.publisher.TopicStatsExposer;
import com.inmobi.stats.StatsExposer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;

public class GraphiteStatsEmitter extends RunnableStatsEmitter {

  public static final String METRIC_SEPARATOR = ".";
  public static final String FIELD_SEPARATOR = " ";
  private static final String NEW_LINE = "\n";
  private static final Log LOG = LogFactory.getLog(GraphiteStatsEmitter.class);

  private String metricPrefix;
  private String graphiteHost;
  private int graphitePort;

  @Override
  public void init(Properties props) {
    sleep = Integer.valueOf(props.getProperty("graphite.emitter.poll.interval",
                                              "10000"));
    metricPrefix = props.getProperty("graphite.emitter.metric.prefix", "");
    if (!metricPrefix.isEmpty()) {
      metricPrefix = metricPrefix + METRIC_SEPARATOR;
    }
    graphiteHost = props.getProperty("graphite.emitter.hostname");
    if (null == graphiteHost || graphiteHost.isEmpty()) {
      throw new IllegalArgumentException("graphite.emitter.hostname cannot be null or empty");
    }
    graphitePort = Integer.parseInt(props.getProperty("graphite.emitter.port", "2003"));
    createThread();
  }

  protected void writeStats() {
    if (null != statsExposers) {
      synchronized (statsExposers) {
        final StringBuilder lines = new StringBuilder();
        long timestamp = System.currentTimeMillis() / 1000;
        for (StatsExposer exposer : statsExposers) {
          Map<String, Number> stats = exposer.getStats();
          Map<String, String> context = exposer.getContexts();
          String topic = context.get(TopicStatsExposer.TOPIC_CONTEXT_NAME);
          for (Map.Entry<String, Number> entry : stats.entrySet()) {
            lines.append(metricPrefix).append(topic).append(METRIC_SEPARATOR)
                .append(entry.getKey());
            lines.append(FIELD_SEPARATOR);
            lines.append(entry.getValue().longValue());
            lines.append(FIELD_SEPARATOR);
            lines.append(timestamp);
            lines.append(NEW_LINE);
          }
        }

        Socket graphiteSocket = null;
        OutputStream stream = null;
        try {
          graphiteSocket = new Socket(graphiteHost, graphitePort);
          stream = graphiteSocket.getOutputStream();
          stream.write(lines.toString().getBytes(Charset.forName("UTF-8")));
        } catch (IOException ex) {
          LOG.error("Failed to write the stats", ex);
        } finally {
          if (null != graphiteSocket && !graphiteSocket.isClosed()) {
            try {
              graphiteSocket.close();
            } catch (IOException ex) {
              LOG.warn("failure in closing the connection to graphite server", ex);
            }
          }
          if (null != stream) {
            try {
              stream.close();
            } catch (IOException ex) {
              LOG.warn("failure in closing the input stream", ex);
            }
          }
        }
      }
    }
  }
}
