package com.inmobi.messaging.util;

/*
 * #%L
 * messaging-client-core
 * %%
 * Copyright (C) 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
