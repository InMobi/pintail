package com.inmobi.messaging.util;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.audit.Column;
import com.inmobi.messaging.consumer.audit.Filter;
import com.inmobi.messaging.consumer.audit.LatencyColumns;
import com.inmobi.messaging.consumer.audit.Tuple;
import com.mysql.jdbc.Driver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.Date;
import java.util.*;

public class AuditDBHelper {

  private static final String AUDIT_DB_CONF_FILE = "audit-db-conf.properties";
  private static final Log LOG = LogFactory.getLog(AuditDBHelper.class);

  private static Connection getConnection(String url, String username,
                                          String password) {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (Exception e) {
      LOG.error("Exception while registering jdbc driver ", e);
    }
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection("jdbc:mysql://" + url, username, password);
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      LOG.error("Exception while creating db connection ", e);
    }
    return connection;
  }

  public static boolean update(Set<Tuple> tupleSet, String confFileName) {

    ClientConfig config;
    if (confFileName == null || confFileName.isEmpty())
      config = ClientConfig.loadFromClasspath(AUDIT_DB_CONF_FILE);
    else
      config = ClientConfig.loadFromClasspath(confFileName);

    LOG.info("Connecting to DB ...");
    Connection connection =
        getConnection(config.getString(AuditDBConstants.DB_URL),
            config.getString(AuditDBConstants.DB_USERNAME),
            config.getString(AuditDBConstants.DB_PASSWORD));
    if (connection == null) {
      LOG.error("Connection not initialized returning ...");
      return false;
    }
    LOG.info("Connected to DB");

    ResultSet rs = null;
    String selectstatement =
        "select * from " + AuditDBConstants.TABLE_NAME + " where " +
            AuditDBConstants.TIMESTAMP + " = ? and " +
            AuditDBConstants.HOSTNAME + " = ? and " + AuditDBConstants.TOPIC +
            " = ? and " + AuditDBConstants.TIER + " = ? and " +
            AuditDBConstants.CLUSTER + " = ?";
    LOG.debug("Select statement: " + selectstatement);
    String columnString = "";
    String setString = "";
    for (int i = 0; i < LatencyColumns.values().length; i++) {
      setString += " and ?= ?";
      columnString += ", ?";
    }
    String insertStatement =
        "insert into " + AuditDBConstants.TABLE_NAME + " (" +
            AuditDBConstants.TIMESTAMP + "," + AuditDBConstants.HOSTNAME +
            ", " + AuditDBConstants.TIER + ", " + AuditDBConstants.TOPIC +
            ", " + AuditDBConstants.CLUSTER + ", " + AuditDBConstants.RECEIVED +
            ", " + AuditDBConstants.SENT + ", " + columnString +
            ") values (?, ?, ?, ?, ?, ?, ?" + columnString + ")";
    LOG.debug("Insert statement: " + insertStatement);
    String updateStatement = "update " + AuditDBConstants.TABLE_NAME + " set " +
        AuditDBConstants.RECEIVED + " = ? and " + AuditDBConstants.SENT +
        " = ?" + setString + " where " + AuditDBConstants.HOSTNAME +
        " = ? and " + AuditDBConstants.TIER + " = ? and " +
        AuditDBConstants.TOPIC + " = ? and " + AuditDBConstants.CLUSTER +
        " = ? and " + AuditDBConstants.TIMESTAMP + " = ? ";
    LOG.debug("Update statement: " + updateStatement);
    PreparedStatement selectPreparedStatement = null, insertPreparedStatement =
        null, updatePreparedStatement = null;
    try {
      selectPreparedStatement = connection.prepareStatement(selectstatement);
      insertPreparedStatement = connection.prepareStatement(insertStatement);
      updatePreparedStatement = connection.prepareStatement(updateStatement);
      for (Tuple tuple : tupleSet) {
        selectPreparedStatement.setLong(1, tuple.getTimestamp().getTime());
        selectPreparedStatement.setString(2, tuple.getHostname());
        selectPreparedStatement.setString(3, tuple.getTopic());
        selectPreparedStatement.setString(4, tuple.getTier());
        selectPreparedStatement.setString(5, tuple.getCluster());
        rs = selectPreparedStatement.executeQuery();
        Long received = 0l;
        if (rs.next()) {
          LOG.debug("Updating tuple in DB:" + tuple);
          Map<LatencyColumns, Long> latencyCountMap =
              new HashMap<LatencyColumns, Long>();
          latencyCountMap.putAll(tuple.getLatencyCountMap());
          for (LatencyColumns latencyColumn : LatencyColumns.values()) {
            Long currentVal = latencyCountMap.get(latencyColumn);
            Long prevVal = rs.getLong(latencyColumn.toString());
            if (prevVal == null)
              prevVal = 0l;
            if (currentVal == null)
              currentVal = 0l;
            if (prevVal > 0l && currentVal == 0l) {
              latencyCountMap.put(latencyColumn, prevVal);
            } else if (prevVal > 0l && currentVal > 0l) {
              LOG.warn("Possible data replay for tuple: " + tuple.toString() +
                  "; Column " + latencyColumn.toString() + " had value " +
                  rs.getLong(latencyColumn.toString()) + " before updation");
              latencyCountMap.put(latencyColumn, currentVal);
            } else {
              latencyCountMap.put(latencyColumn, currentVal);
            }
            received += latencyCountMap.get(latencyColumn);
          }
          Long sent = tuple.getSent() + rs.getLong(AuditDBConstants.SENT);
          updatePreparedStatement.setLong(1, received);
          updatePreparedStatement.setLong(2, sent);
          int index = 3;
          for (Map.Entry<LatencyColumns, Long> entry : latencyCountMap
              .entrySet()) {
            updatePreparedStatement
                .setString(index++, entry.getKey().toString());
            updatePreparedStatement.setLong(index++, entry.getValue());
          }
          updatePreparedStatement.setString(index++, tuple.getHostname());
          updatePreparedStatement.setString(index++, tuple.getTier());
          updatePreparedStatement.setString(index++, tuple.getTopic());
          updatePreparedStatement.setString(index++, tuple.getCluster());
          updatePreparedStatement
              .setLong(index++, tuple.getTimestamp().getTime());
          LOG.debug("Update prepared statement : " +
              updatePreparedStatement.toString());
          updatePreparedStatement.addBatch();
        } else {
          //no record in db corresponding to this tuple
          LOG.debug("Inserting tuple in DB " + tuple);
          insertPreparedStatement.setLong(1, tuple.getTimestamp().getTime());
          insertPreparedStatement.setString(2, tuple.getHostname());
          insertPreparedStatement.setString(3, tuple.getTier());
          insertPreparedStatement.setString(4, tuple.getTopic());
          insertPreparedStatement.setString(5, tuple.getCluster());
          insertPreparedStatement.setLong(6, tuple.getReceived());
          insertPreparedStatement.setLong(7, tuple.getSent());
          Map<LatencyColumns, Long> latencyCountMap =
              tuple.getLatencyCountMap();
          int index = 8, numberColumns = LatencyColumns.values().length;
          for (LatencyColumns latencyColumn : LatencyColumns.values()) {
            insertPreparedStatement.setString(index, latencyColumn.toString());
            Long count = latencyCountMap.get(latencyColumn);
            if (count == null)
              count = 0l;
            insertPreparedStatement.setLong(index + numberColumns, count);
            index++;
          }
          LOG.debug("Insert prepared statement : " +
              insertPreparedStatement.toString());
          insertPreparedStatement.addBatch();
        }
      }
      updatePreparedStatement.executeBatch();
      insertPreparedStatement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      LOG.error("SQLException thrown ", e);
      return false;
    } finally {
      try {
        rs.close();
        selectPreparedStatement.close();
        insertPreparedStatement.close();
        updatePreparedStatement.close();
        connection.close();
      } catch (SQLException e) {
        LOG.warn("Exception while closing ", e);
      }
    }
    return true;
  }

  public static Set<Tuple> retrieve(Date toDate, Date fromDate, Filter filter,
                                    String confFileName) {
    Set<Tuple> tupleSet = new HashSet<Tuple>();

    ClientConfig config;
    if (confFileName == null || confFileName.isEmpty())
      config = ClientConfig.loadFromClasspath(AUDIT_DB_CONF_FILE);
    else
      config = ClientConfig.loadFromClasspath(confFileName);

    LOG.info("Connecting to DB ...");
    Connection connection =
        getConnection(config.getString(AuditDBConstants.DB_URL),
            config.getString(AuditDBConstants.DB_USERNAME),
            config.getString(AuditDBConstants.DB_PASSWORD));
    if (connection == null) {
      LOG.error("Connection not initialized returning ...");
      return null;
    }
    LOG.info("Connected to DB");

    ResultSet rs = null;
    String hostname = filter.getFilters().get(Column.HOSTNAME);
    String tier = filter.getFilters().get(Column.TIER);
    String topic = filter.getFilters().get(Column.TOPIC);
    String cluster = filter.getFilters().get(Column.CLUSTER);
    String sumString = "";
    String asString = "";
    String whereString = "";
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      sumString += ", Sum(" + latencyColumn.toString() + ")";
      asString += ", " + latencyColumn.toString();
    }
    for (int i = 0; i < filter.getFilters().size(); i++) {
      whereString += " and ? = ?";
    }
    String statement =
        "select " + AuditDBConstants.HOSTNAME + ", " + AuditDBConstants.TIER +
            ", " + AuditDBConstants.TOPIC + ", " + AuditDBConstants.CLUSTER +
            ", Sum(" + AuditDBConstants.RECEIVED + "), Sum(" +
            AuditDBConstants.SENT + ")" + sumString + " as " +
            AuditDBConstants.HOSTNAME + ", " + AuditDBConstants.TIER + ", " +
            AuditDBConstants.TOPIC + ", " + AuditDBConstants.CLUSTER + ", " +
            AuditDBConstants.RECEIVED + ", " + AuditDBConstants.SENT +
            asString + " from " + AuditDBConstants.TABLE_NAME + " where " +
            AuditDBConstants.TIMESTAMP + " >= ? and " +
            AuditDBConstants.TIMESTAMP + " < ? " + whereString + " group by " +
            AuditDBConstants.HOSTNAME + ", " + AuditDBConstants.TIER + ", " +
            AuditDBConstants.TOPIC + ", " + AuditDBConstants.CLUSTER;
    LOG.debug("Select statement " + statement);
    PreparedStatement preparedstatement = null;
    try {
      preparedstatement = connection.prepareStatement(statement);
      preparedstatement.setLong(1, fromDate.getTime());
      preparedstatement.setLong(2, toDate.getTime());
      int index = 3;
      if (hostname != null || !hostname.isEmpty()) {
        preparedstatement.setString(index++, AuditDBConstants.HOSTNAME);
        preparedstatement.setString(index++, hostname);
      }
      if (tier != null || !tier.isEmpty()) {
        preparedstatement.setString(index++, AuditDBConstants.TIER);
        preparedstatement.setString(index++, tier);
      }
      if (topic != null || !topic.isEmpty()) {
        preparedstatement.setString(index++, AuditDBConstants.TOPIC);
        preparedstatement.setString(index++, topic);
      }
      if (cluster != null || !cluster.isEmpty()) {
        preparedstatement.setString(index++, AuditDBConstants.CLUSTER);
        preparedstatement.setString(index++, cluster);
      }
      LOG.debug("Prepared statement is " + preparedstatement.toString());
      rs = preparedstatement.executeQuery();
      while (rs.next()) {
        Tuple tuple = new Tuple(rs.getString(AuditDBConstants.HOSTNAME),
            rs.getString(AuditDBConstants.TIER),
            rs.getString(AuditDBConstants.CLUSTER),
            rs.getTimestamp(AuditDBConstants.TIMESTAMP),
            rs.getString(AuditDBConstants.TOPIC),
            rs.getLong(AuditDBConstants.RECEIVED),
            rs.getLong(AuditDBConstants.SENT));
        Map<LatencyColumns, Long> latencyCountMap =
            new HashMap<LatencyColumns, Long>();
        for (LatencyColumns latencyColumn : LatencyColumns.values()) {
          latencyCountMap
              .put(latencyColumn, rs.getLong(latencyColumn.toString()));
        }
        tuple.setLatencyCountMap(latencyCountMap);
        tupleSet.add(tuple);
      }
      connection.commit();
    } catch (SQLException e) {
      LOG.error("SQLException encountered", e);
    } finally {
      try {
        rs.close();
        preparedstatement.close();
        connection.close();
      } catch (SQLException e) {
        LOG.warn("Exception while closing ", e);
      }
    }
    return tupleSet;
  }
}
