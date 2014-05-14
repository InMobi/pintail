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

public class DatabusStreamFile implements StreamFile {

  private String collectorName;
  private CollectorFile collectorFile;
  private String extension;

  public DatabusStreamFile(String collectorName,
                           CollectorFile collectorFile,
                           String extension) {
    this.collectorName = collectorName;
    this.collectorFile = collectorFile;
    this.extension = extension;
  }

  /**
   * Used only in case of serialization
   */
  public DatabusStreamFile() {
  }
  public static DatabusStreamFile create(String streamName, String fileName) {
    String[] strs = fileName.split(streamName);
    if (strs.length < 2) {
      throw new IllegalArgumentException("Invalid file name:" + fileName);
    }
    if (strs[0].length() == 0) {
      throw new IllegalArgumentException("Invalid file name:" + fileName);
    }
    String collectorName = strs[0].substring(0, strs[0].length() - 1);

    String cfSubString = fileName.substring(collectorName.length() + 1);
    String[] str2 = cfSubString.split("\\.");

    if (str2.length < 2) {
      throw new IllegalArgumentException("Invalid file name:" + fileName);
    }
    CollectorFile collectorFile =  CollectorFile.create(str2[0]);
    String extension = str2[1];

    return new DatabusStreamFile(collectorName, collectorFile, extension);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((collectorFile == null) ? 0 : collectorFile.hashCode());
    result = prime * result + ((collectorName == null) ? 0 : collectorName.hashCode());
    result = prime * result + ((extension == null) ? 0 : extension.hashCode());
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
    DatabusStreamFile other = (DatabusStreamFile) obj;

    if (collectorFile == null) {
      if (other.collectorFile != null) {
        return false;
      }
    } else if (!collectorFile.equals(other.collectorFile)) {
      return false;
    }
    if (collectorName == null) {
      if (other.collectorName != null) {
        return false;
      }
    } else if (!collectorName.equals(other.collectorName)) {
      return false;
    }
    if (extension == null) {
      if (other.extension != null) {
        return false;
      }
    } else if (!extension.equals(other.extension)) {
      return false;
    }

    return true;
  }

  public String toString() {
    return collectorName + "-" + collectorFile.toString() + "." + extension;
  }

  @Override
  public int compareTo(Object o) {
    DatabusStreamFile other = (DatabusStreamFile) o;
    int cfComp = collectorFile.compareTo(other.collectorFile);
    if (cfComp == 0) {
      int cnComp = collectorName.compareTo(other.collectorName);
      if (cnComp == 0) {
        return extension.compareTo(other.extension);
      } else {
        return cnComp;
      }
    } else {
      return cfComp;
    }
  }

  public CollectorFile getCollectorFile() {
    return collectorFile;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(collectorName);
    collectorFile.write(out);
    out.writeUTF(extension);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    collectorName = in.readUTF();
    collectorFile = new CollectorFile();
    collectorFile.readFields(in);
    extension = in.readUTF();
  }

}
