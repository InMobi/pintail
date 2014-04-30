package random.pkg;

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

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.thrift.TException;

import com.facebook.fb303.fb_status;

import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe.Iface;

public class ScribeSlackOnce implements Iface {

  private Log LOG;

  public ScribeSlackOnce() {
    LOG = LogFactory.getLog("scribeserver");
  }

  @Override
  public String getName() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getVersion() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public fb_status getStatus() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getStatusDetails() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getCounter(String key) throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setOption(String key, String value) throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public String getOption(String key) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCpuProfile(int profileDurationInSec) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long aliveSince() throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void reinitialize() throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void shutdown() throws TException {
    // TODO Auto-generated method stub

  }

  boolean slackonce = true;
  @Override
  public ResultCode Log(List<LogEntry> messages) throws TException {
    if (LOG.isInfoEnabled()) {
      for (LogEntry m : messages) {
        LOG.info(m.getCategory() + ":" + m.getMessage());
      }
    }
    if (slackonce) {
      slackonce = false;
      try {
        System.err.println("server is slacking");
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new TException(e);
      }
    }
    return ResultCode.OK;
  }
}
