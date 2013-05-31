package com.inmobi.messaging.util;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.audit.*;
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
    String selectstatement = getSelectStmtForUpdation();
    String insertStatement = getInsertStmtForUpdation();
    String updateStatement = getUpdateStmtForUpdation();
    PreparedStatement selectPreparedStatement = null, insertPreparedStatement =
        null, updatePreparedStatement = null;
    try {
      selectPreparedStatement = connection.prepareStatement(selectstatement);
      insertPreparedStatement = connection.prepareStatement(insertStatement);
      updatePreparedStatement = connection.prepareStatement(updateStatement);
      for (Tuple tuple : tupleSet) {
        rs = executeSelectStmtUpdation(selectPreparedStatement, tuple);
        if (rs.next()) {
          if (!addToUpdateStatementBatch(updatePreparedStatement, tuple, rs))
            return false;
        } else {
          if (!addToInsertStatementBatch(insertPreparedStatement, tuple))
            return false;
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

  private static ResultSet executeSelectStmtUpdation(
      PreparedStatement selectPreparedStatement, Tuple tuple) {
    int  i = 1;
    ResultSet rs;
    try {
      selectPreparedStatement.setLong(i++, tuple.getTimestamp().getTime());
      selectPreparedStatement.setString(i++, tuple.getHostname());
      selectPreparedStatement.setString(i++, tuple.getTopic());
      selectPreparedStatement.setString(i++, tuple.getTier());
      selectPreparedStatement.setString(i++, tuple.getCluster());
      rs = selectPreparedStatement.executeQuery();
    } catch (SQLException e) {
      LOG.error("Exception encountered ", e);
      return null;
    }
    return rs;
  }

  private static String getUpdateStmtForUpdation() {
    String setString = "";
    for (int i = 0; i < LatencyColumns.values().length-1; i++) {
      setString += " and ?= ?";
    }
    String updateStatement = "update " + AuditDBConstants.TABLE_NAME + " set " +
        "" + AuditDBConstants.SENT + " = ?" + setString + " where " + Column
        .HOSTNAME + " = ? and " + Column.TIER + " = ? and " + Column.TOPIC +
        " = ? and " + Column.CLUSTER + " = ? and " + AuditDBConstants
        .TIMESTAMP + " = ? ";
    LOG.debug("Update statement: " + updateStatement);
    return updateStatement;
  }

  private static String getInsertStmtForUpdation() {
    String columnString = "";
    for (int i = 0; i < LatencyColumns.values().length-1; i++) {
      columnString += ", ?";
    }
    String insertStatement =
        "insert into " + AuditDBConstants.TABLE_NAME + " (" +
            AuditDBConstants.TIMESTAMP + "," + Column.HOSTNAME +
            ", " + Column.TIER + ", " + Column.TOPIC +
            ", " + Column.CLUSTER + ", " + AuditDBConstants.SENT  +
            columnString + ") values (?, ?, ?, ?, ?, ?, " +
            "?" + columnString + ")";
    LOG.debug("Insert statement: " + insertStatement);
    return insertStatement;
  }

  public static String getSelectStmtForUpdation() {
    String selectstatement =
        "select * from " + AuditDBConstants.TABLE_NAME + " where " +
            AuditDBConstants.TIMESTAMP + " = ? and " + Column.HOSTNAME + " = " +
            "? and " + Column.TOPIC + " = ? and " + Column.TIER + "" +
            " = ? and " + Column.CLUSTER + " = ?";
    LOG.debug("Select statement: " + selectstatement);
    return selectstatement;
  }

  private static boolean addToInsertStatementBatch(
      PreparedStatement insertPreparedStatement, Tuple tuple) {
    try {
      LOG.debug("Inserting tuple in DB " + tuple);
      int index = 1;
      insertPreparedStatement.setLong(index++, tuple.getTimestamp().getTime());
      insertPreparedStatement.setString(index++, tuple.getHostname());
      insertPreparedStatement.setString(index++, tuple.getTier());
      insertPreparedStatement.setString(index++, tuple.getTopic());
      insertPreparedStatement.setString(index++, tuple.getCluster());
      insertPreparedStatement.setLong(index++, tuple.getSent());
      Map<LatencyColumns, Long> latencyCountMap =
          tuple.getLatencyCountMap();
      int numberColumns = LatencyColumns.values().length;
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
    } catch (SQLException e) {
      LOG.error("Exception thrown while adding to insert statement batch", e);
      return false;
    }
    return true;
  }

  private static boolean addToUpdateStatementBatch(
      PreparedStatement updatePreparedStatement, Tuple tuple, ResultSet rs) {
    try {
      LOG.debug("Updating tuple in DB:" + tuple);
      Map<LatencyColumns, Long> latencyCountMap =
          new HashMap<LatencyColumns, Long>();
      latencyCountMap.putAll(tuple.getLatencyCountMap());
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        Long currentVal = latencyCountMap.get(latencyColumn);
        Long prevVal = rs.getLong(latencyColumn.toString());
        Long count = getCountForLatency(currentVal, prevVal, latencyColumn, tuple);
        latencyCountMap.put(latencyColumn, count);
      }
      Long sent = tuple.getSent() + rs.getLong(AuditDBConstants.SENT);
      int index = 1;
      updatePreparedStatement.setLong(index++, sent);
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        updatePreparedStatement.setString(index++, latencyColumn.toString());
        updatePreparedStatement.setLong(index++, latencyCountMap.get(
            latencyColumn));
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
    } catch (SQLException e) {
      LOG.error("Exception thrown while adding to batch of update statement",
          e);
      return false;
    }
    return true;
  }

  private static Long getCountForLatency(Long currentVal, Long prevVal,
                                         LatencyColumns latencyColumn,
                                         Tuple tuple) {
    Long count;
    if (prevVal == null)
      prevVal = 0l;
    if (currentVal == null)
      currentVal = 0l;
    if (prevVal > 0l && currentVal == 0l) {
      count = prevVal;
    } else if (prevVal > 0l && currentVal > 0l) {
      LOG.error("Possible data replay for tuple: " + tuple.toString() +
          "; Column " + latencyColumn.toString() + " had value " +
          prevVal + " before updation");
      count = currentVal;
    } else {
      count = currentVal;
    }
    return count;
  }

  public static Set<Tuple> retrieve(Date toDate, Date fromDate, Filter filter,
                                    GroupBy groupBy, String confFileName) {
    LOG.debug("Retrieving from db  from-time :" + fromDate + " to-date :" +
        ":" + toDate + " filter :" + filter.toString() +
        " and conf-filename :" + confFileName);
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
    String statement = getSelectStmtForRetrieve(filter, groupBy);
    PreparedStatement preparedstatement = null;
    try {
      preparedstatement = connection.prepareStatement(statement);
      int index = 1;
      preparedstatement.setLong(index++, fromDate.getTime());
      preparedstatement.setLong(index++, toDate.getTime());
      for (Column column : Column.values()) {
        String value = filter.getFilters().get(column);
        if ( value != null || !value.isEmpty() ) {
          preparedstatement.setString(index++, column.toString());
          preparedstatement.setString(index++, value);
        }
      }
      LOG.debug("Prepared statement is " + preparedstatement.toString());
      rs = preparedstatement.executeQuery();
      while (rs.next()) {
        Tuple tuple = createNewTuple(rs, groupBy);
        if (tuple == null) {
          LOG.error("Returned null tuple..returning");
          return null;
        }
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

  private static Tuple createNewTuple(ResultSet rs, GroupBy groupBy) {
    Tuple tuple;
    try {
      Map<Column, String> columnValuesInTuple = new HashMap<Column, String>();
      for (Column column:Column.values()) {
        if (groupBy.getGroupByColumns().contains(column))
          columnValuesInTuple.put(column, rs.getString(column.toString()));
      }
      Map<LatencyColumns, Long> latencyCountMap =
          new HashMap<LatencyColumns, Long>();
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        latencyCountMap
            .put(latencyColumn, rs.getLong(latencyColumn.toString()));
      }
      tuple = new Tuple(columnValuesInTuple.get(Column.HOSTNAME),
          columnValuesInTuple.get(Column.TIER), columnValuesInTuple.get
          (Column.CLUSTER), null, columnValuesInTuple.get(Column.TOPIC),
          latencyCountMap, rs.getLong(AuditDBConstants.SENT));
    } catch (SQLException e) {
      LOG.error("Exception thrown while creating new tuple ", e);
      return null;
    }
    return tuple;
  }

  private static String getSelectStmtForRetrieve(Filter filter, GroupBy groupBy) {
    String sumString = "", asString = "", whereString = "", groupByString = "";
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      sumString += ", Sum(" + latencyColumn.toString() + ")";
      asString += ", " + latencyColumn.toString();
    }
    for (int i = 0; i < filter.getFilters().size(); i++) {
      whereString += " and ? = ?";
    }
    for (Column column : groupBy.getGroupByColumns()) {
      if (!groupByString.isEmpty()) {
        groupByString += ", "+column.toString();
      } else {
        groupByString += column.toString();
      }
    }
    String statement =
        "select " + groupByString + ", Sum(" + AuditDBConstants.SENT + ")" +
            sumString + " as " + groupByString + ", " + AuditDBConstants.SENT
            + asString + " from " + AuditDBConstants.TABLE_NAME + " where " +
            AuditDBConstants.TIMESTAMP + " >= ? and " + AuditDBConstants
            .TIMESTAMP + " < ? " + whereString + " group by " + groupByString;
    LOG.debug("Select statement " + statement);
    return statement;
  }
}
