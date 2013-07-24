package com.inmobi.databus.files;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

public class TestHadoopStreamFile {

  protected Path rootDir = new Path("/tmp/test/hadoop/",
      this.getClass().getSimpleName());
  FileSystem fs;

  @Test
  public void testHadoopStreamFile() throws IOException {
    fs = FileSystem.getLocal(new Configuration());
    Path p1 = new Path(rootDir, "2012/12/12/12/11");
    Path p2 = new Path(rootDir, "2012/12/12/12/12");
    Long t1 = 1L;
    Long t2 = 2L;
    String f1 = "f1";
    String f2 = "f2";
    HadoopStreamFile hp1 = new HadoopStreamFile(p1, null, null);
    HadoopStreamFile hp2 = new HadoopStreamFile(p2, null, null);
    HadoopStreamFile h11 = new HadoopStreamFile(p1, f1, t1);
    HadoopStreamFile h12 = new HadoopStreamFile(p1, f2, t2);
    HadoopStreamFile h21 = new HadoopStreamFile(p2, f1, t1);
    HadoopStreamFile h22 = new HadoopStreamFile(p2, f2, t2);

    Assert.assertTrue(hp1.equals(h11));
    Assert.assertTrue(hp1.equals(h12));
    Assert.assertTrue(hp2.equals(h21));
    Assert.assertTrue(hp2.equals(h22));
    Assert.assertTrue(hp1.compareTo(h21) < 0);
    Assert.assertTrue(hp1.compareTo(h21) < 0);
    Assert.assertTrue(h11.compareTo(h12) < 0);
    Assert.assertTrue(h12.compareTo(h21) < 0);
    Assert.assertTrue(h21.compareTo(h22) < 0);

    fs.mkdirs(p1);
    Path pf11 = new Path(p1, f1);
    fs.create(pf11);
    FileStatus fs11 = fs.getFileStatus(pf11);
    Assert.assertEquals(HadoopStreamFile.create(fs11).toString(), new Path(
        "2012/12/12/12/11/f1").toString());

    fs.delete(p1, true);
  }

  @Test
  public void testCeiling() {
    TreeMap<HadoopStreamFile, String> fileMap = new TreeMap<HadoopStreamFile, String>();
    fileMap.put(new HadoopStreamFile(new Path(
        "hdfs://localhost:9000/databus/streams_local/stream1/2013/05/29/08/46/"),
        "localhost-stream1-2013-05-29-08-44_00000.gz", 1369817168983L), "1");
    fileMap.put(new HadoopStreamFile(new Path(
        "hdfs://localhost:9000/databus/streams_local/stream1/2013/05/29/08/46/"),
        "localhost-stream1-2013-05-29-08-45_00000.gz", 1369817169526L), "2");
    fileMap.put(new HadoopStreamFile(new Path(
        "hdfs://localhost:9000/databus/streams_local/stream1/2013/05/29/08/46/"),
        "localhost-stream1-2013-05-29-08-45_00001.gz", 1369817169889L), "3");
    fileMap.put(new HadoopStreamFile(new Path(
        "hdfs://localhost:9000/databus/streams_local/stream1/2013/05/29/08/46/"),
        "localhost-stream1-2013-05-29-08-45_00002.gz", 1369817170221L), "4");
    fileMap.put(new HadoopStreamFile(new Path(
        "hdfs://localhost:9000/databus/streams_local/stream1/2013/05/29/08/46/"),
        "localhost-stream1-2013-05-29-08-45_00003.gz", 1369817170517L), "5");
    fileMap.put(new HadoopStreamFile(new Path(
        "hdfs://localhost:9000/databus/streams_local/stream1/2013/05/29/08/46/"),
        "localhost-stream1-2013-05-29-08-45_00004.gz", 1369817240828L), "6");
    HadoopStreamFile timeKey = new HadoopStreamFile(new Path(
        "hdfs://localhost:9000/databus/streams_local/stream1/2013/05/29/08/46/"),
        null, null);
    Assert.assertEquals(fileMap.ceilingEntry(timeKey), fileMap.firstEntry());
  }

  @AfterTest
  public void cleanUp() throws IOException {
    fs.delete(rootDir, true);
  }
}
