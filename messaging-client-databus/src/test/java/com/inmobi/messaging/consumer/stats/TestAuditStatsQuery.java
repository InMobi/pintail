package com.inmobi.messaging.consumer.stats;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.testng.annotations.Test;

import com.inmobi.audit.thrift.AuditPacket;

public class TestAuditStatsQuery {

  private void createData(Path dirPath) throws IOException, TException {
    Date date = new Date();
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.MINUTE, 1);
    Date nextdate = cal.getTime();

    FileSystem fs = FileSystem.getLocal(new Configuration());
    cal.setTime(date);
    String suffix = cal.get(Calendar.YEAR) + File.separator
        + (cal.get(Calendar.MONTH) + 1) + File.separator
        + cal.get(Calendar.DAY_OF_MONTH) + File.separator
        + cal.get(Calendar.HOUR_OF_DAY)
        + File.separator + cal.get(Calendar.MINUTE);
    cal.setTime(nextdate);
    String nextSuffix = cal.get(Calendar.YEAR) + File.separator
        + cal.get(Calendar.MONTH) + File.separator
        + cal.get(Calendar.DAY_OF_MONTH) + cal.get(Calendar.HOUR_OF_DAY)
        + File.separator + cal.get(Calendar.MINUTE);
    Path p1 = new Path(dirPath, suffix);
    Path p2 = new Path(dirPath, nextSuffix);
    Map<Long, Long> received = new HashMap<Long, Long>();
    long time = System.currentTimeMillis();
    long window = time - time % 60000;
    received.put(window, 100l);
    AuditPacket packet = new AuditPacket(System.currentTimeMillis(),
        "testTopic", "publisher", "localhost", 1, received, received);
    TSerializer serializer = new TSerializer();
    // serializer.serialize(packet);
    byte[] output = Base64.encodeBase64(serializer.serialize(packet));
    FSDataOutputStream out = fs.create(new Path(p1, "a"));
    out.write(output);
    out.close();
  }

  @Test
  public void testAuditStats() throws IOException, TException {
    Path dirPath = new Path("file:///tmp/test/databustest1/streams/audit");
    createData(dirPath);
    String args[] = new String[4];
    args[0] = "-conf";
    args[1] = "src/test/resources/databus-consumer-conf.properties";
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date());
    calendar.add(Calendar.MINUTE, -5);
    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm");

    args[2] = formatter.format(calendar.getTime());
    calendar.setTime(new Date());
    args[3] = formatter.format(calendar.getTime());
    AuditStatsQuery.main(args);
  }

}
