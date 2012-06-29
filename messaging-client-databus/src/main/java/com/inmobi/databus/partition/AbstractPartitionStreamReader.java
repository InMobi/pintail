package com.inmobi.databus.partition;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.inmobi.databus.readers.StreamReader;

public abstract class AbstractPartitionStreamReader implements 
     PartitionStreamReader {

  protected StreamReader reader;
  protected boolean closed = false;
  
  protected StreamReader getReader() {
    return this.reader;
  }

  public Path getCurrentFile() {
    return reader.getCurrentFile();
  }

  @Override
  public long getCurrentLineNum() {
    return reader.getCurrentLineNum();
  }

  @Override
  public void openStream() throws IOException {
    reader.openStream();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    reader.close();
  }

  @Override
  public void closeStream() throws IOException {
    reader.closeStream();    
  }

  public String readLine() throws IOException, InterruptedException {
    return reader.readLine();
  }

}
