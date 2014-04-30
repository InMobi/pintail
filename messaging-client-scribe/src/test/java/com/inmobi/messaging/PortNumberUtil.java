package com.inmobi.messaging;

/*
 * #%L
 * messaging-client-scribe
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
