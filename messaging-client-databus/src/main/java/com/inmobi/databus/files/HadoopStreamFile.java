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
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class HadoopStreamFile implements StreamFile {

  //fileName can be null
  private String fileName;
  private Path parent;
  //file creation time
  private Long timeStamp;
  private String checkpointPath;

  /**
   * Used only during serialization
   */
  public HadoopStreamFile() {
  }

  public HadoopStreamFile(Path parent, String fileName, Long timeStamp) {
    this.fileName = fileName;
    this.parent = parent;
    this.timeStamp = timeStamp;
    constructCheckpointPath();
  }

  static String minDirFormatStr = "yyyy" + File.separator + "MM"
      + File.separator + "dd" + File.separator + "HH" + File.separator + "mm";

  public void constructCheckpointPath() {
    String parentDir = parent.toString();
    String[] str = parentDir.split("[0-9]{4}.[0-9]{2}.[0-9]{2}.[0-9]{2}.[0-9]{2}");
    checkpointPath = parentDir.substring(str[0].length());
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
    if (timeStamp != null && other.timeStamp != null) {
      if (!timeStamp.equals(other.timeStamp)) {
        return false;
      }
    }
    if (fileName != null && other.fileName != null) {
      if (!fileName.equals(other.fileName)) {
        return false;
      }
    }
    return true;
  }

  public String toString() {
    return checkpointPath + (fileName == null ? "" : File.separator + fileName);
  }

  @Override
  public int compareTo(Object o) {
    HadoopStreamFile other = (HadoopStreamFile) o;
    int cComp = checkpointPath.compareTo(other.checkpointPath);
    if (cComp == 0) {
      if (timeStamp != null) {
        if (other.timeStamp != null) {
          int tComp = timeStamp.compareTo(other.timeStamp);
          if (tComp == 0) {
            if (fileName != null && other.fileName != null) {
              return fileName.compareTo(other.fileName);
            }
          } else {
            return tComp;
          }
        } else {
          return 1;
        }
      } else {
        if (other.timeStamp != null) {
          return -1;
        }
      }
    }
    return cComp;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(parent.toString());
    boolean notNull = fileName != null;
    out.writeBoolean(notNull);
    if (notNull) {
      out.writeUTF(fileName);
    }
    notNull = timeStamp != null;
    out.writeBoolean(notNull);
    if (notNull) {
      out.writeLong(timeStamp);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    boolean migrate = Boolean.parseBoolean(
        System.getProperty("consumer.checkpoint.migrate", "false"));
    if (migrate) {
      String strPath = in.readUTF();
      this.parent = new Path(strPath);
      this.fileName = in.readUTF();
      this.timeStamp = in.readLong();
      constructCheckpointPath(); 
    } else {
    String strPath = in.readUTF();
    this.parent = new Path(strPath);
    boolean notNull = in.readBoolean();
    if (notNull) {
      this.fileName = in.readUTF();
    }
    notNull = in.readBoolean();
    if (notNull) {
      this.timeStamp = in.readLong();
    }
    constructCheckpointPath();
    }
  }

  public Path getParent() {
    return parent;
  }

  public Long getTimestamp() {
    return timeStamp;
  }

  public String getFileName() {
    return fileName;
  }

  public String getCheckpointPath() {
    return checkpointPath;
  }
}
