package com.inmobi.messaging.consumer.audit;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.thrift.TException;
import org.testng.annotations.Test;

import com.inmobi.messaging.consumer.audit.AuditStatsQuery;

public class TestAuditStatsQuery {


  @Test
  public void testAuditStats() throws IOException, TException {
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
