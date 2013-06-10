package com.inmobi.messaging.util;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.audit.*;
import junit.framework.Assert;
import org.apache.log4j.Priority;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class TestAuditDBHelper {
  String configFile = "audit-db-conf.properties";
  Connection connection;
  Tuple tuple1, tuple2, tuple3, tuple4;
  Set<Tuple> tupleSet1, tupleSet2, tupleSet3;

  @BeforeTest
  public void setup() {
    ClientConfig config = ClientConfig.loadFromClasspath(configFile);
    connection =
        AuditDBHelper.getConnection(
            config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
            config.getString(AuditDBConstants.DB_URL),
            config.getString(AuditDBConstants.DB_USERNAME),
            config.getString(AuditDBConstants.DB_PASSWORD));
    Assert.assertTrue( connection != null );
    String createTable = "CREATE TABLE audit(\n  TIMEINTERVAL bigint,\n  HOSTNAME varchar(25),\n  TIER varchar(15),\n  TOPIC varchar(25),\n  CLUSTER varchar(50),\n  SENT bigint,\n  C0 bigint,\n  C1 bigint,\n  C2 bigint,\n  C3 bigint,\n  C4 bigint,\n  C5 bigint,\n  C6 bigint,\n  C7 bigint,\n  C8 bigint,\n  C9 bigint,\n  C10 bigint,\n  C15 bigint,\n  C30 bigint,\n  C60 bigint,\n  C120 bigint,\n  C240 bigint,\n  C600 bigint\n)";
    try {
      connection.prepareStatement(createTable).execute();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    createTuples();
  }

  private void createTuples() {
    /*
     Add tuples in tuplesets so that insert, update are possible
     */
    String hostname1 = "testhost1";
    String hostname2 = "testhost2";
    String tier = Tier.AGENT.toString();
    String cluster = "testCluster";
    Date timestamp = new Date(1355314332l);
    String topic = "testTopic";
    String topic2 = "testTopic";
    Map<LatencyColumns, Long> latencyCountMap1 = new HashMap<LatencyColumns,
        Long>();
    Map<LatencyColumns, Long> latencyCountMap2 = new HashMap<LatencyColumns,
        Long>();
    Map<LatencyColumns, Long> latencyCountMap3 = new HashMap<LatencyColumns,
        Long>();
    latencyCountMap1.put(LatencyColumns.C1, 500l);
    latencyCountMap1.put(LatencyColumns.C0, 1500l);
    latencyCountMap2.put(LatencyColumns.C1, 1000l);
    latencyCountMap2.put(LatencyColumns.C2, 1000l);
    latencyCountMap2.put(LatencyColumns.C3, 500l);
    latencyCountMap3.put(LatencyColumns.C600, 1000l);
    Long sent1 = 2000l;
    Long sent2 = 2500l;
    Long sent3 = 1000l;

    tuple1 = new Tuple(hostname1, tier, cluster, timestamp, topic,
        latencyCountMap1, sent1);
    tuple2 = new Tuple(hostname1, tier, cluster, timestamp, topic,
        latencyCountMap2, sent2);
    tuple3 = new Tuple(hostname1, tier, cluster, timestamp, topic2,
        latencyCountMap3, sent3);
    tuple4 = new Tuple(hostname2, tier, cluster, timestamp, topic,
        latencyCountMap1, sent1);

    tupleSet1 = new HashSet<Tuple>();
    tupleSet2 = new HashSet<Tuple>();
    tupleSet3 = new HashSet<Tuple>();
    tupleSet1.add(tuple1);
    tupleSet2.add(tuple2);
    tupleSet3.add(tuple3);
    tupleSet3.add(tuple4);
  }

  @AfterTest
  public void shutDown() {
    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
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
    Date fromDate = new Date(1355314200l);
    Date toDate = new Date(1355314400l);
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
