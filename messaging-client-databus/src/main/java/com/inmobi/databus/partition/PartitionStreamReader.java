package com.inmobi.databus.partition;

import java.io.IOException;

import com.inmobi.databus.files.StreamFile;

public interface PartitionStreamReader {

  void initializeCurrentFile() throws IOException, InterruptedException;
  
  StreamFile getCurrentFile();
  
  long getCurrentLineNum();

  void openStream() throws IOException;
  
  String readLine() throws IOException, InterruptedException;
  
  void closeStream() throws IOException;
  
  void close() throws IOException;
}
