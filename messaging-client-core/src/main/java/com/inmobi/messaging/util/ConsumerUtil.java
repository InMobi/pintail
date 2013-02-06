package com.inmobi.messaging.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public class ConsumerUtil {
  
  private static final Log LOG = LogFactory.getLog(ConsumerUtil.class);
  private static final byte[] magicBytes = {(byte)0xAB,(byte)0xCD,(byte)0xEF};
  private static final byte[] versions = { 1 };
  private static final int HEADER_LENGTH = 16;

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
  
  public static ByteBuffer removeHeader(byte data[]) {
    boolean isValidHeaders = true;
    if (data.length < 16) {
      LOG.debug("Total size of data in message is less than length of headers");
      isValidHeaders = false;
    }
    ByteBuffer buffer = ByteBuffer.wrap(data);
    boolean isVersionValid = false;
    if (isValidHeaders) {
      for (byte version : versions) {
        if (buffer.get() == version) {
          isVersionValid = true;
          break;
        }
      }
      if (isVersionValid) {
        // compare all 3 magicBytes
        byte[] mBytesRead = new byte[3];
        buffer.get(mBytesRead);
        if (mBytesRead[0] != magicBytes[0] || mBytesRead[1] != magicBytes[1]
            || mBytesRead[2] != magicBytes[2])
          isValidHeaders = false;
      } else {
        LOG.debug("Invalid version in the headers");
    }
      if (isValidHeaders) {
        // TODO add validation for timestamp
        long timestamp = buffer.getLong();

        int messageSize = buffer.getInt();
        if (isValidHeaders && data.length != HEADER_LENGTH + messageSize) {
          isValidHeaders = false;
          LOG.debug("Invalid size of messag in headers");
        }
      }
    }


    if (isValidHeaders) {
      return ByteBuffer.wrap(Arrays.copyOfRange(data, HEADER_LENGTH,
          data.length));
  }
 else
      return ByteBuffer.wrap(data);
}
}