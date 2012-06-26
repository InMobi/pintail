package com.inmobi.databus.partition;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface PartitionStreamReader {

  void initializeCurrentFile() throws IOException, InterruptedException;
  
  Path getCurrentFile();
  
  long getCurrentLineNum();

  void openStream() throws IOException;
  
  String readLine() throws IOException, InterruptedException;
  
  void closeStream() throws IOException;
  
  void close() throws IOException;
}
