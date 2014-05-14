package com.inmobi.databus.files;

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
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CollectorFile implements StreamFile {
  private String streamName;
  private String timestamp;
  private int id;

  private  static final NumberFormat idFormat = NumberFormat.getInstance();

  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(5);
  }

  public static final ThreadLocal<DateFormat> fileFormat =
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + "-" + "MM" + "-" + "dd" + "-"
          + "HH" + "-" + "mm");
    }
  };

  public CollectorFile(String streamName, Date timestamp, int id) {
    this.streamName = streamName;
    this.timestamp = fileFormat.get().format(timestamp);
    this.id = id;
  }

  /**
   * Used only during serialization
   */
  public CollectorFile() {
  }

  public static CollectorFile create(String fileName) {
    String [] strs = fileName.split("-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}");
    if (strs.length < 2) {
      throw new IllegalArgumentException("Invalid file name:" + fileName);
    }
    String streamName = strs[0];
    String dateSubStr = fileName.substring(streamName.length() + 1);
    String[] str2 = dateSubStr.split("_");
    if (str2.length < 2) {
      throw new IllegalArgumentException("Invalid file name:" + fileName);
    }
    Date timestamp = null;
    try {
      timestamp =  fileFormat.get().parse(str2[0]);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Invalid file name:" + fileName, e);
    }
    int id = Integer.parseInt(str2[1]);

    return new CollectorFile(streamName, timestamp, id);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((streamName == null) ? 0 : streamName.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    result = prime * result + Integer.valueOf(id).hashCode();
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
    CollectorFile other = (CollectorFile) obj;
    if (streamName == null) {
      if (other.streamName != null) {
        return false;
      }
    } else if (!streamName.equals(other.streamName)) {
      return false;
    }
    if (timestamp == null) {
      if (other.timestamp != null) {
        return false;
      }
    } else if (!timestamp.equals(other.timestamp)) {
      return false;
    }
    if (id != other.id) {
      return false;
    }
    return true;
  }

  public String toString() {
    return streamName + "-" + timestamp + "_"
        + idFormat.format(id);
  }

  @Override
  public int compareTo(Object o) {
    CollectorFile other = (CollectorFile) o;
    return this.toString().compareTo(other.toString());
  }

  public Date getTimestamp() {
    try {
      return fileFormat.get().parse(timestamp);
    } catch (ParseException e) {
      return null;
    }
  }

  public String getStreamName() {
    return streamName;
  }

  public int getId() {
    return id;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(streamName);
    out.writeUTF(timestamp);
    out.writeInt(id);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.streamName = in.readUTF();
    this.timestamp = in.readUTF();
    this.id = in.readInt();
  }
}
