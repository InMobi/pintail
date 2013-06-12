package com.inmobi.messaging.util;

import com.inmobi.messaging.consumer.audit.*;
import junit.framework.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class TestAuditDBHelper extends  AuditDBUtil {

  @BeforeTest
  public void setup() {
    setupDB(false);
  }

  @AfterTest
  public void shutDown() {
    super.shutDown();
  }

  @Test(priority = 1)
  public void testUpdate() {
    int index = 1;
    String selectStmt = AuditDBHelper.getSelectStmtForUpdation();
    PreparedStatement selectStatement = null;
    ResultSet rs = null;
    try {
      selectStatement = connection.prepareStatement(selectStmt);
      boolean isSuccessful = AuditDBHelper.update(tupleSet1, null);
      Assert.assertTrue(isSuccessful);
      selectStatement.setLong(index++, tuple1.getTimestamp().getTime());
      selectStatement.setString(index++, tuple1.getHostname());
      selectStatement.setString(index++, tuple1.getTopic());
      selectStatement.setString(index++, tuple1.getTier());
      selectStatement.setString(index++, tuple1.getCluster());
      rs = selectStatement.executeQuery();
      Assert.assertTrue(rs.next());
      Assert.assertEquals(tuple1.getSent(), rs.getLong(AuditDBConstants.SENT));
      for (LatencyColumns latencyColumns : LatencyColumns.values()) {
        Long val = tuple1.getLatencyCountMap().get(latencyColumns);
        if (val == null)
          val = 0l;
        Assert.assertEquals(val, (Long) rs.getLong(latencyColumns.toString()));
      }
      Assert.assertEquals(tuple1.getLostCount(),
          (Long) rs.getLong(LatencyColumns.C600.toString()));
      isSuccessful = AuditDBHelper.update(tupleSet2, null);
      Assert.assertTrue(isSuccessful);
      index = 1;
      selectStatement.setLong(index++, tuple1.getTimestamp().getTime());
      selectStatement.setString(index++, tuple1.getHostname());
      selectStatement.setString(index++, tuple1.getTopic());
      selectStatement.setString(index++, tuple1.getTier());
      selectStatement.setString(index++, tuple1.getCluster());
      rs = selectStatement.executeQuery();
      Assert.assertTrue(rs.next());
      Assert.assertEquals(tuple1.getSent() + tuple2.getSent(),
          rs.getLong(AuditDBConstants.SENT));
      for (LatencyColumns latencyColumns : LatencyColumns.values()) {
        Long val1 = tuple1.getLatencyCountMap().get(latencyColumns);
        if (val1 == null)
          val1 = 0l;
        Long val2 = tuple2.getLatencyCountMap().get(latencyColumns);
        if (val2 == null)
          val2 = 0l;
        Assert.assertEquals(val1 + val2, rs.getLong(latencyColumns
            .toString()));
      }
      Assert.assertEquals(tuple1.getLostCount() + tuple2.getLostCount(),
          rs.getLong(LatencyColumns.C600.toString()));
      isSuccessful = AuditDBHelper.update(tupleSet3, null);
      Assert.assertTrue(isSuccessful);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (selectStatement != null) {
          selectStatement.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  @Test(priority = 2)
  public void testRetrieve() {
    GroupBy groupBy = new GroupBy("TIER,HOSTNAME,CLUSTER");
    Filter filter = new Filter("hostname="+tuple1.getHostname());
    Set<Tuple> tupleSet = AuditDBHelper.retrieve(toDate, fromDate, filter,
        groupBy, null);
    Assert.assertEquals(1, tupleSet.size());
    Iterator<Tuple> tupleSetIter = tupleSet.iterator();
    Assert.assertTrue(tupleSetIter.hasNext());
    Tuple returnedTuple = tupleSetIter.next();
    Assert.assertEquals(tuple1.getHostname(), returnedTuple.getHostname());
    Assert.assertEquals(tuple1.getTier(), returnedTuple.getTier());
    Assert.assertEquals(null, returnedTuple.getTopic());
    Assert.assertEquals(tuple1.getSent() + tuple2.getSent() + tuple3.getSent(),
        returnedTuple.getSent());
    for (LatencyColumns latencyColumns : LatencyColumns.values()) {
      Long val1 = tuple1.getLatencyCountMap().get(latencyColumns);
      if (val1 == null)
        val1 = 0l;
      Long val2 = tuple2.getLatencyCountMap().get(latencyColumns);
      if (val2 == null)
        val2 = 0l;
      Long val3 = tuple3.getLatencyCountMap().get(latencyColumns);
      if (val3 == null)
        val3 = 0l;
      Long val4 = returnedTuple.getLatencyCountMap().get(latencyColumns);
      if (val4 == null)
        val4 = 0l;
      Long valx = val1 + val2 + val3;
      Assert.assertEquals(valx, val4);
    }
    Assert.assertEquals((Long) (tuple1.getLostCount() + tuple2.getLostCount() +
        tuple3.getLostCount()), returnedTuple.getLostCount());
    filter = new Filter(null);
    tupleSet = AuditDBHelper.retrieve(toDate, fromDate, filter, groupBy, null);
    Assert.assertEquals(2, tupleSet.size());
  }
}
