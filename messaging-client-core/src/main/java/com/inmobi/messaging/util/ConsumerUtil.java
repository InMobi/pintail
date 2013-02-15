package com.inmobi.messaging.util;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class ConsumerUtil {

  public static Date getCurrenDateForTimeZone(String timezone) {
    TimeZone tz = TimeZone.getTimeZone(timezone);
    Calendar tzCal = new GregorianCalendar(tz);
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR, tzCal.get(Calendar.YEAR));
    cal.set(Calendar.MONTH, tzCal.get(Calendar.MONTH));
    cal.set(Calendar.DAY_OF_MONTH, tzCal.get(Calendar.DAY_OF_MONTH));
    cal.set(Calendar.HOUR_OF_DAY, tzCal.get(Calendar.HOUR_OF_DAY));
    cal.set(Calendar.MINUTE, tzCal.get(Calendar.MINUTE));
    cal.set(Calendar.SECOND, tzCal.get(Calendar.SECOND));
    cal.set(Calendar.MILLISECOND, tzCal.get(Calendar.MILLISECOND));

    return cal.getTime();
  }
}
