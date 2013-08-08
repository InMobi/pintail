package com.inmobi.messaging;

import java.io.IOException;
import java.net.ServerSocket;

public class PortNumberUtil {

  public static int getFreePortNumber(int defaultPortNumber) {
    int port = defaultPortNumber;
    try {
      ServerSocket socket = new ServerSocket(0);
      port = socket.getLocalPort();
    } catch (IOException e) {
      System.out.println("IOException occured while creating socket."
          + " Use a default port number " +  port);
    }
    return port;
  }
}
