package com.inmobi.messaging.util;

import com.inmobi.messaging.consumer.audit.Filter;
import com.inmobi.messaging.consumer.audit.Tuple;

import java.util.Date;
import java.util.List;

public class AuditDBHelper {
  public static boolean update(List<Tuple> tupleList) {
    return true;
  }

  public static List<Tuple> retrieve(Date toDate, Date fromDate,
                                     Filter filter) {
    return null;
  }
}
