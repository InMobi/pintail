package random.pkg;

/*
 * #%L
 * messaging-client-scribe
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

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import scribe.thrift.scribe;
import scribe.thrift.scribe.Iface;

public class SimpleSocketServer {
  private int port = 7911;
  private TServerTransport serverTransport;
  private TServer server;
  private Iface handler;

  private Thread t;

  public SimpleSocketServer(int port) {
    this(port, new ScribeAlwaysSuccess());
  }

  public SimpleSocketServer(int port, Iface handler) {
    this.handler = handler;
    this.port = port;
  }

  public synchronized void start() throws TTransportException {
    if (server != null)
      return;

    serverTransport = new TServerSocket(port);
    scribe.Processor processor = new scribe.Processor(handler);
    TProtocolFactory protFactory = new TBinaryProtocol.Factory(true, true);
    server = new TThreadPoolServer(processor, serverTransport,
        new TFramedTransport.Factory(), protFactory);
    t = new Thread() {
      public void run() {
        server.serve();
      }
    };
    t.start();
  }

  public synchronized void stop() {
    if (server != null) {
      server.stop();
      serverTransport.close();
      server = null;
      serverTransport = null;
      t.stop();
    }
  }

}
