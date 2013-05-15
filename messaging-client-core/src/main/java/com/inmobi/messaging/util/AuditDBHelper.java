package com.inmobi.messaging.util;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.audit.Column;
import com.inmobi.messaging.consumer.audit.Filter;
import com.inmobi.messaging.consumer.audit.LatencyColumns;
import com.inmobi.messaging.consumer.audit.Tuple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class AuditDBHelper implements AuditDBConstants {

  private static final String DB_TABLE_CONF_FILE = "db-table-conf.properties";
  private static final Log LOG = LogFactory.getLog(AuditDBHelper.class);

  private static Connection getConnection() {
    ClientConfig config = ClientConfig.loadFromClasspath(DB_TABLE_CONF_FILE);
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
    } catch (Exception e) {
      LOG.error("Exception while loading jdbc driver ", e);
    }
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection("jdbc:mysql://" + config.getString(DB_URL),
              config.getString(DB_USERNAME), config.getString(DB_PASSWORD));
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      LOG.error("Exception while creating db connection ", e);
    }
    return connection;
  }

  public static void update(List<Tuple> tupleList) {
    LOG.info("Connecting to DB ...");
    Connection connection = getConnection();
    if (connection == null) {
      return;
    }
    LOG.info("Connected to DB");
    ResultSet rs = null;
    String selectstatement = "select * from " + TABLE_NAME + " where " +
        "" + TIMESTAMP + " = ? and " + HOSTNAME + " = ? and " + TOPIC +
        " = ? and " + TIER + " = ? and " + CLUSTER + " = ?";
    String columnList = "", valueList = "";
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      columnList += ", " + latencyColumn.toString();
      valueList += ", ?";
    }
    String insertStatement =
        "insert into " + TABLE_NAME + " (" + TIMESTAMP + ", " +
            HOSTNAME + ", " + TIER + ", " + TOPIC + ", " + CLUSTER +
            columnList + ") values (?, ?, " +
            ", ?, ?" + valueList + ")";
    String setString = "";
    for (int i = 0; i < LatencyColumns.values().length; i++) {
      if (setString.isEmpty()) {
        setString += " ? = ?";
      } else {
        setString += " and ?= ?";
      }
    }
    String updateStatement =
        "update " + TABLE_NAME + " set " + setString + " where " +
            "" + HOSTNAME + " = ? and " + TIER + " = ? and " + TOPIC +
            " = ? and " + CLUSTER + " " +
            "= ? and " + TIMESTAMP + " = ? ";

    try {
      PreparedStatement selectPreparedStatement =
          connection.prepareStatement(selectstatement);
      PreparedStatement insertPreparedStatement =
          connection.prepareStatement(insertStatement);
      PreparedStatement updatePreparedStatement =
          connection.prepareStatement(updateStatement);
      for (Tuple tuple : tupleList) {
        selectPreparedStatement.setLong(1, tuple.getTimestamp().getTime());
        selectPreparedStatement.setString(2, tuple.getHostname());
        selectPreparedStatement.setString(3, tuple.getTopic());
        selectPreparedStatement.setString(4, tuple.getTier());
        selectPreparedStatement.setString(5, tuple.getCluster());
        rs = selectPreparedStatement.executeQuery();
        if (rs.next()) {
          Map<LatencyColumns, Long> latencyCountMap =
              new TreeMap<LatencyColumns, Long>();
          latencyCountMap.putAll(tuple.getLatencyCountMap());
          for (LatencyColumns latencyColumn : LatencyColumns.values()) {
            Long prevVal = latencyCountMap.get(latencyColumn);
            if (prevVal != null) {
              latencyCountMap.put(latencyColumn,
                  rs.getLong(latencyColumn.toString()) + prevVal);
            } else {
              latencyCountMap
                  .put(latencyColumn, rs.getLong(latencyColumn.toString()));
            }
          }
          int index = 1;
          for (Map.Entry<LatencyColumns, Long> entry : latencyCountMap
              .entrySet()) {
            updatePreparedStatement.setString(index, entry.getKey().toString());
            index++;
            updatePreparedStatement.setLong(index, entry.getValue());
            index++;
          }
          updatePreparedStatement.setString(index, tuple.getHostname());
          index++;
          updatePreparedStatement.setString(index, tuple.getTier());
          index++;
          updatePreparedStatement.setString(index, tuple.getTopic());
          index++;
          updatePreparedStatement.setString(index, tuple.getCluster());
          index++;
          updatePreparedStatement
              .setLong(index, tuple.getTimestamp().getTime());
          updatePreparedStatement.addBatch();
        } else {
          //no record in db corresponding to this tuple
          insertPreparedStatement.setLong(1, tuple.getTimestamp().getTime());
          insertPreparedStatement.setString(2, tuple.getHostname());
          insertPreparedStatement.setString(3, tuple.getTier());
          insertPreparedStatement.setString(4, tuple.getTopic());
          insertPreparedStatement.setString(5, tuple.getCluster());
          Map<LatencyColumns, Long> latencyCountMap =
              new TreeMap<LatencyColumns, Long>();
          latencyCountMap.putAll(tuple.getLatencyCountMap());
          int index = 6;
          for (Map.Entry<LatencyColumns, Long> entry : latencyCountMap
              .entrySet()) {
            insertPreparedStatement.setLong(index, entry.getValue());
            index++;
          }
          insertPreparedStatement.addBatch();
        }
      }
      updatePreparedStatement.executeBatch();
      insertPreparedStatement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      LOG.error("SQLException thrown ", e);
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        LOG.warn("Exception while closing ", e);
      }
    }
  }

  public static List<Tuple> retrieve(Date toDate, Date fromDate,
                                     Filter filter) {
    List<Tuple> tupleList = new ArrayList<Tuple>();
    LOG.info("Connecting to DB ...");
    Connection connection = getConnection();
    if (connection == null) {
      return null;
    }
    LOG.info("Connected to DB");
    ResultSet rs = null;
    String hostname = filter.getFilters().get(Column.HOSTNAME);
    String tier = filter.getFilters().get(Column.TIER);
    String topic = filter.getFilters().get(Column.TOPIC);
    String cluster = filter.getFilters().get(Column.CLUSTER);
    String statement =
        "select * from " + TABLE_NAME + " where " + TIMESTAMP + " >= ?" +
            " and " + TIMESTAMP + " <= ?";
    for (int i = 0; i < filter.getFilters().size(); i++) {
      statement += " and ? = ?";
    }
    try {
      PreparedStatement preparedstatement =
          connection.prepareStatement(statement);
      preparedstatement.setLong(1, fromDate.getTime());
      preparedstatement.setLong(2, toDate.getTime());
      int index = 3;
      if (hostname != null || !hostname.isEmpty()) {
        preparedstatement.setString(index, HOSTNAME);
        index++;
        preparedstatement.setString(index, hostname);
        index++;
      }
      if (tier != null || !tier.isEmpty()) {
        preparedstatement.setString(index, TIER);
        index++;
        preparedstatement.setString(index, tier);
        index++;
      }
      if (topic != null || !topic.isEmpty()) {
        preparedstatement.setString(index, TOPIC);
        index++;
        preparedstatement.setString(index, topic);
        index++;
      }
      if (cluster != null || !cluster.isEmpty()) {
        preparedstatement.setString(index, CLUSTER);
        index++;
        preparedstatement.setString(index, cluster);
        index++;
      }
      LOG.info("Prepared statement is " + preparedstatement.toString());
      rs = preparedstatement.executeQuery();
      while (rs.next()) {
        Tuple tuple = new Tuple(rs.getString(HOSTNAME), rs.getString(TIER),
            rs.getString(CLUSTER), rs.getTimestamp(TIMESTAMP),
            rs.getString(TOPIC));
        Map<LatencyColumns, Long> latencyCountMap =
            new TreeMap<LatencyColumns, Long>();
        for (LatencyColumns latencyColumn : LatencyColumns.values()) {
          latencyCountMap
              .put(latencyColumn, rs.getLong(latencyColumn.toString()));
        }
        tuple.setLatencyCountMap(latencyCountMap);
        tupleList.add(tuple);
      }
      connection.commit();
    } catch (SQLException e) {
      LOG.error("SQLException encountered", e);
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        LOG.warn("Exception while closing ", e);
      }
    }
    return tupleList;
  }
}
