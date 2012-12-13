package com.inmobi.databus.files;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHadoopStreamFile {
  
  @Test
  public void testHadoopStreamFile() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path p1 = new Path("/tmp/test/2012/12/12/12/11");
    Path p2 = new Path("/tmp/test/2012/12/12/12/12");
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

}
