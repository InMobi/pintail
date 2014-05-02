package com.inmobi.databus.partition;

/*
 * #%L
 * messaging-client-databus
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.inmobi.databus.files.HadoopStreamFile;
import org.apache.hadoop.io.Writable;

import com.inmobi.databus.files.StreamFile;
import com.inmobi.messaging.consumer.InvalidCheckpointException;
import com.inmobi.messaging.consumer.databus.MessageCheckpoint;

public class PartitionCheckpoint implements Writable, MessageCheckpoint {
  private StreamFile streamFile;
  private long lineNum;

  public PartitionCheckpoint(StreamFile streamFile, long lineNum) {
    this.streamFile = streamFile;
    this.lineNum = lineNum;
  }

  public PartitionCheckpoint(DataInput in) throws IOException {
    readFields(in);
  }

  public String getFileName() {
    return streamFile.toString();
  }

  public String getName() {
    if ( streamFile instanceof HadoopStreamFile)
      return ((HadoopStreamFile) streamFile).getFileName();
    else
      return getFileName();
  }

  public StreamFile getStreamFile() {
    return streamFile;
  }

  public long getLineNum() {
    return lineNum;
  }

  @Override
  public boolean isNULL() {
    return false;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String streamFileClassName = in.readUTF();
    Class<?> clazz;
    try {
      clazz = Class.forName(streamFileClassName);
      streamFile = (StreamFile) clazz.newInstance();
    } catch (Exception e) {
      throw new InvalidCheckpointException("Invalid stream file class", e);
    }
    streamFile.readFields(in);
    lineNum = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(streamFile.getClass().getCanonicalName());
    streamFile.write(out);
    out.writeLong(lineNum);
  }

  public String toString() {
    return streamFile + "-" + lineNum;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((streamFile == null) ? 0 : streamFile.hashCode());
    result = prime * result + (int) (lineNum ^ (lineNum >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PartitionCheckpoint other = (PartitionCheckpoint) obj;
    if (streamFile == null) {
      if (other.streamFile != null) {
        return false;
      }
    } else if (!streamFile.equals(other.streamFile)) {
      return false;
    }
    if (lineNum != other.lineNum) {
      return false;
    }
    return true;
  }

}
