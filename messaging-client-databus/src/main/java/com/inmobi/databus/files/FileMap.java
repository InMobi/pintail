package com.inmobi.databus.files;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public abstract class FileMap<T extends StreamFile> {
  private static final Log LOG = LogFactory.getLog(FileMap.class);

  protected TreeMap<T, Path> files;
  protected Iterator<T> fileNameIterator;
  protected PathFilter pathFilter;

  public FileMap() {
    this.pathFilter = createPathFilter();
  }

  protected abstract TreeMap<T, Path> createFilesMap();

  protected abstract T getStreamFile(Path file);

  protected abstract T getStreamFile(String fileName);

  protected abstract PathFilter createPathFilter(); 

  protected abstract void buildList() throws IOException;


  private void createIterator() {
    fileNameIterator = files.navigableKeySet().iterator();    
  }

  public void addPath(Path path) {
    T fileKey = getStreamFile(path);
    files.put(fileKey, path);
    LOG.info("Added path: " + path);
  }

  public Path getCeilingValue(String fileName) {
    T fileKey = getStreamFile(fileName);
    Map.Entry<T, Path> ceilingEntry = files.ceilingEntry(fileKey);
    if (ceilingEntry != null) {
      return ceilingEntry.getValue();
    } else {
      return null;
    }
  }

  public Path getHigherValue(Path file)
      throws IOException {
    T fileKey = getStreamFile(file);
    return getHigherValue(fileKey);
  }

  public Path getHigherValue(String fileName)
      throws IOException {
    T fileKey = getStreamFile(fileName);
    return getHigherValue(fileKey);
  }

  private Path getHigherValue(T fileKey)
      throws IOException {
    Map.Entry<T, Path> higherEntry = files.higherEntry(fileKey);
    if (higherEntry != null) {
      return higherEntry.getValue();
    } else {
      return null;
    }
  }

  public Path getValue(String fileName) {
    T fileKey = getStreamFile(fileName);
    return files.get(fileKey);
  }

  public Path getValue(StreamFile fileKey) {
    return files.get(fileKey);
  }

  public void build() throws IOException {
    files = createFilesMap();
    buildList();
    createIterator();
  }

  public boolean isEmpty() {
    return files.isEmpty();
  }

  public Path getFirstFile()
      throws IOException {
    Map.Entry<T, Path> first = files.firstEntry();
    if (first != null) {
      return first.getValue();
    }
    return null;
  }

  public boolean containsFile(String fileName) {
    return files.containsKey(getStreamFile(fileName)); 
  }

  public boolean setIterator(Path cfile) {
    createIterator();
    if (cfile != null) {
      while (fileNameIterator.hasNext()) {
        StreamFile file = fileNameIterator.next();
        if (cfile.getName().equals(file.toString())) {
          return true;
        } 
      }
    } 
    LOG.info("Did not find file" + cfile);
    return false;
  }

  public boolean isBefore(String fileName) throws IOException {
    if (!isEmpty()
        && getFirstFile().getName().compareTo(fileName) > 0) {
      return true;
    }
    return false;
  }

  public boolean isWithin(String fileName) throws IOException {
    if (!isEmpty()
        && getFirstFile().getName().compareTo(fileName) < 1) {
      return true;
    }
    return false;
  }

  public Path getNext() {
    if (fileNameIterator.hasNext()) {
      StreamFile fileName = fileNameIterator.next();
      LOG.debug("next file name:" + fileName);
      return getValue(fileName);
    }
    return null;
  }
}
