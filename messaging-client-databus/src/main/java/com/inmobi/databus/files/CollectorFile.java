package com.inmobi.databus.files;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CollectorFile implements StreamFile {
  private final String streamName;
  private final String timestamp;
  private final int id;

  private  static final NumberFormat idFormat = NumberFormat.getInstance();

  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(5);
  }

  public static final ThreadLocal<DateFormat> fileFormat = 
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + "-" + "MM" + "-" + "dd" + "-" +
          "HH" + "-" + "mm");
    }    
  };

  public CollectorFile(String streamName, Date timestamp, int id) {
    this.streamName = streamName;
    this.timestamp = fileFormat.get().format(timestamp);
    this.id = id;
  }
  
  public static CollectorFile create(String fileName) {
    String strs[] = fileName.split("-");
    if (strs.length < 2) {
      throw new IllegalArgumentException("Invalid file name:" + fileName);
    }
    String streamName = strs[0];
    String dateSubStr = fileName.substring(streamName.length() + 1);
    String str2[] = dateSubStr.split("_");
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
    CollectorFile other = (CollectorFile)o;
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
}
