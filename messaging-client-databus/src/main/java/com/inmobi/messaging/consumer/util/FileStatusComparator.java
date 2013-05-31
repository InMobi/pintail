package com.inmobi.messaging.consumer.util;

import java.util.Comparator;

import org.apache.hadoop.fs.FileStatus;

public class FileStatusComparator implements Comparator<FileStatus>{

  public int compare(FileStatus o1, FileStatus o2) {
    if (o1.getPath().getName().compareTo(o2.getPath().getName()) < 0) {
      return 1;
    }
    return -1;
  }
}