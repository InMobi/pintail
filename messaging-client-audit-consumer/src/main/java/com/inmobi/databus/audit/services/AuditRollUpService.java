package com.inmobi.databus.audit.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.audit.AuditService;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.util.AuditDBConstants;
import com.inmobi.messaging.util.AuditDBHelper;

public class AuditRollUpService extends AuditService {

  final private ClientConfig config;
  final private int rollUpHourOfDay;
  final private long intervalLength;
  final private String checkPointDir;
  private final String destnTableName, srcTableName;
  final private int tilldays;
  private static String ROLLUP_HOUR_KEY = "feeder.rollup.hour";
  private static String INTERVAL_LENGTH_KEY = "feeder.rollup.intervallength.millis";
  private static String CHECKPOINT_DIR_KEY = "feeder.rollup.checkpoint.dir";
  private static String CHECKPOINT_KEY = "feeder";
  private static String TILLDAYS_KEY = "feeder.rollup.tilldays";
  private static String MONTHLY_TABLE_KEY = "feeder.rollup.table.monthly";
  private static String DAILY_TABLE_KEY = "feeder.rollup.table.daily";
  private static final Log LOG = LogFactory.getLog(AuditRollUpService.class);
  public AuditRollUpService(ClientConfig config) {
    this.config = config;
    rollUpHourOfDay = config.getInteger(ROLLUP_HOUR_KEY, 0);
    intervalLength = config.getLong(INTERVAL_LENGTH_KEY, 3600000l);
    checkPointDir = config.getString(CHECKPOINT_DIR_KEY);
    tilldays = config.getInteger(TILLDAYS_KEY);
    destnTableName = config.getString(MONTHLY_TABLE_KEY);
    srcTableName = config.getString(DAILY_TABLE_KEY);
  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub

  }

  private Long getToTime() {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, -tilldays);
    cal.set(Calendar.HOUR_OF_DAY, 23);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.SECOND, 59);
    cal.set(Calendar.MILLISECOND, 59);
    return cal.getTimeInMillis();
  }

  private long getTimeToSleep(){
    Calendar cal =Calendar.getInstance();
    long currentTime = cal.getTimeInMillis();
    // setting calendar to rollup hour
    if (cal.get(Calendar.HOUR_OF_DAY) >= rollUpHourOfDay) {
      // rollup will happen the next day
    cal.add(Calendar.DAY_OF_MONTH, 1);
    }
    cal.set(Calendar.HOUR_OF_DAY, rollUpHourOfDay);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    long rollUpTime = cal.getTimeInMillis();
    return rollUpTime - currentTime;
  }

  private String getRollUpQuery() {
    String query = "select rollup(?,?,?,?,?)";
    return query;
  }

  private String formatDate(Date date) {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    return formatter.format(date);
  }
  private String getDailyTableName(Date date) {
    return srcTableName + formatDate(date);
  }

  private String getMonthlyTableName(Date date) {
    return destnTableName + formatDate(date);
  }

  private Long getFromTime() {
    FSCheckpointProvider provider = new FSCheckpointProvider(checkPointDir);
    byte[] value = provider.read(CHECKPOINT_KEY);
    if (value == null)
      return 0l;
    return Long.parseLong(new String(value));
  }

  private void mark(Long toTime) {
    FSCheckpointProvider provider = new FSCheckpointProvider(checkPointDir);
    provider.checkpoint(CHECKPOINT_KEY, toTime.toString().getBytes());
    // TODO alter upper constraint of destn table and drop src table partition
  }
  @Override
  public void execute() {
    // wait till the roll up hour
    long waitTime = getTimeToSleep();
    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      LOG.warn("RollUp Service interrupted", e);
    }

    Long fromTime = getFromTime();
    Long toTime = getToTime();
    LOG.info("Connecting to DB ...");
    Connection connection = AuditDBHelper.getConnection(
        config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
        config.getString(AuditDBConstants.DB_URL),
        config.getString(AuditDBConstants.DB_USERNAME),
        config.getString(AuditDBConstants.DB_PASSWORD));
    if (connection == null) {
      LOG.error("Connection not initialized returning ...");
      return;
    }
    LOG.info("Connected to DB");
    // TODO create new destination partition table if doesn't exist
    String statement = getRollUpQuery();
    PreparedStatement preparedstatement = null;
    try {
      preparedstatement = connection.prepareStatement(statement);
      int index = 1;
      preparedstatement.setString(index++, destnTableName);
      preparedstatement.setString(index++, srcTableName);
      preparedstatement.setLong(index++, fromTime);
      preparedstatement.setLong(index++, toTime);
      preparedstatement.setLong(index++, intervalLength);
      LOG.info("Rollup query is " + preparedstatement.toString());
      preparedstatement.executeQuery();
      connection.commit();
      mark(toTime);
    } catch (SQLException e) {
      LOG.error("Error while rollup", e);
      return;
    }
  }

  @Override
  public String getServiceName() {
    return "RollUpService";
  }

}
