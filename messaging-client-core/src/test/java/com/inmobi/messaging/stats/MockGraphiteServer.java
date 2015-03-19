package com.inmobi.messaging.stats;

import com.inmobi.messaging.util.GraphiteStatsEmitter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class MockGraphiteServer implements Runnable {

  private final Thread serverThread;
  private final ServerSocket serverSocket;
  private volatile boolean shouldRun;
  private Map<String, String> metricMap;
  private StringBuffer stringBuffer;
  private static final Log LOG = LogFactory.getLog(MockGraphiteServer.class);

  public MockGraphiteServer(int port) throws IOException {
    serverThread = new Thread(this);
    serverSocket = new ServerSocket(port);

  }

  public void start() throws IOException {
    metricMap = new HashMap<String, String>();
    stringBuffer = new StringBuffer();
      shouldRun = true;
    if (serverSocket.isClosed()) {
      serverSocket.bind(serverSocket.getLocalSocketAddress());
    }
    serverThread.start();
  }

  public void stop() throws IOException, InterruptedException {
    shouldRun = false;
    if (serverSocket.isBound()) {
      serverSocket.close();
    }
    serverThread.join();

    String[] lines = stringBuffer.toString().split("\n");
    for (String line : lines) {
      String[] fields = line.split(GraphiteStatsEmitter.FIELD_SEPARATOR);
      metricMap.put(fields[0], fields[1]);
    }
  }

  public Map<String, String> getMetricMap() {
    return metricMap;
  }

  @Override
  public void run() {
    while (shouldRun) {
      if (!serverSocket.isClosed()) {
        try {
          Socket connectedSocket = serverSocket.accept();
          collectMetrics(connectedSocket.getInputStream());
        } catch (IOException e) {
          LOG.info(e.getMessage());
        }
      }
    }
  }

  private void collectMetrics(InputStream is) throws IOException {
    BufferedInputStream bis = new BufferedInputStream(is);
    byte[] bb = new byte[1024];
    int size = bis.read(bb);
    while (size >= 0) {
      stringBuffer.append(new String(bb, 0, size, "UTF-8"));
      size = bis.read(bb);
    }

  }
}
