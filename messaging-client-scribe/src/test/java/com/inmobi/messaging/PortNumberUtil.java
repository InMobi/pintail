package com.inmobi.messaging;

import java.io.IOException;
import java.net.ServerSocket;

public class PortNumberUtil {

  public static int getFreePortNumber(int defaultPortNumber) {
    int port = defaultPortNumber;
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      port = socket.getLocalPort();
    } catch (IOException e) {
      System.out.println("IOException occured while creating socket."
          + " Use a default port number " +  port);
    } finally {
      try {
        if (socket != null) {
          socket.close();
        }
      } catch (IOException e) {
        return defaultPortNumber;
      }
    }
    return port;
  }
}
