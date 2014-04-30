package com.inmobi.databus.files;

/*
 * #%L
 * messaging-client-databus
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

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDatabusStreamFile {

  @Test
  public void testDatabusStreamFile() throws ParseException {
    Calendar now = Calendar.getInstance();
    String stream1 = "a";
    String collector1 = "c-1";
    String collector2 = "c-2";
    Date date = now.getTime();
    String dateStr = CollectorFile.fileFormat.get().format(date);
    CollectorFile cf1 = new CollectorFile(stream1, date, 0);
    now.add(Calendar.MINUTE, 1);
    CollectorFile cf2 = new CollectorFile(stream1, now.getTime(), 1);

    DatabusStreamFile df1 = new DatabusStreamFile(collector1, cf1, "gz");
    String df1Str = new String(collector1 + "-" + stream1 + "-" + dateStr +
        "_00000" + ".gz");
    Assert.assertEquals(df1.toString(), df1Str);
    Assert.assertEquals(df1, DatabusStreamFile.create(stream1, df1Str));

    DatabusStreamFile df2 = new DatabusStreamFile(collector2, cf1, "gz");
    DatabusStreamFile df3 = new DatabusStreamFile(collector1, cf2, "gz");
    Assert.assertTrue(df1.hashCode() < df3.hashCode());
    Assert.assertTrue(df1.hashCode() < df2.hashCode());

    Assert.assertTrue(df2.compareTo(df1) > 0);
    Assert.assertTrue(df3.compareTo(df1) > 0);
    Assert.assertTrue(df3.compareTo(df2) > 0);

    Throwable th = null;
    try {
      df1 = DatabusStreamFile.create(stream1, "invalid");
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    th = null;
    try {
      df1 = DatabusStreamFile.create(stream1, cf1.toString());
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    th = null;
    try {
      df1 = DatabusStreamFile.create(stream1, collector1 + "-" + cf1.toString());
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);

    th = null;
    try {
      df1 = DatabusStreamFile.create(stream1, collector1 + "-");
    } catch (Exception e) {
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

}
