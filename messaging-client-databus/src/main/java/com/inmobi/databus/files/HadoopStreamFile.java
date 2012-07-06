package com.inmobi.databus.files;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class HadoopStreamFile implements StreamFile {

  private final String fileName;
  private Path parent;
  //file creation time
  private Long timeStamp;

  public HadoopStreamFile(Path parent, String fileName, Long timeStamp) {
    this.fileName = fileName;
    this.parent = parent;
    this.timeStamp = timeStamp;
  }

  public static HadoopStreamFile create(FileStatus status) {
    return new HadoopStreamFile(status.getPath().getParent(),
        status.getPath().getName(),  status.getModificationTime());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((parent == null) ? 0 : parent.hashCode());
    result = prime * result + ((timeStamp == null) ? 0 : timeStamp.hashCode());
    result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
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
    HadoopStreamFile other = (HadoopStreamFile) obj;

    if (parent == null) {
      if (other.parent != null) {
        return false;
      }
    } else if (!parent.equals(other.parent)) {
      return false;
    }
    if (timeStamp == null) {
      if (other.timeStamp != null) {
        return false;
      }
    } else if (!timeStamp.equals(other.timeStamp)) {
      return false;
    }
    if (fileName == null) {
      if (other.fileName != null) {
        return false;
      }
    } else if (!fileName.equals(other.fileName)) {
      return false;
    }
    return true;
  }

  public String toString() {
    return new Path(parent, fileName).toString();
  }

  @Override
  public int compareTo(Object o) {
    HadoopStreamFile other = (HadoopStreamFile)o;
    int pComp = parent.compareTo(other.parent);
    if ( pComp== 0) {
      int tComp = timeStamp.compareTo(other.timeStamp);
      if ( tComp == 0) {
        return fileName.compareTo(other.fileName);
      } else {
        return tComp;
      }
    } else {
      return pComp;
    }
  }
}
