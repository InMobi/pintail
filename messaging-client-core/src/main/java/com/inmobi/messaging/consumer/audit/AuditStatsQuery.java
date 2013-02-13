package com.inmobi.messaging.consumer.audit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.audit.GroupBy.Group;
import com.inmobi.messaging.util.AuditUtil;

enum Column {
  TIER, HOSTNAME, TOPIC
}


public class AuditStatsQuery {

  Map<Group, Long> received;

  public Map<Group, Long> getReceived() {
    return received;
  }

  public Map<Group, Long> getSent() {
    return sent;
  }

  Map<Group, Long> sent;
  private static final int minArgs = 4;

  private static final Logger LOG = LoggerFactory
      .getLogger(AuditStatsQuery.class);
  Date fromTime;
  Date toTime;
  long cutoffTime = 600000;
  long timeout = 60000;
  private static final String MESSAGE_CLIENT_CONF_FILE = "audit-consumer-conf.properties";
  public static final String ROOT_DIR_KEY = "databus.consumer.rootdirs";
  private boolean isTimeOut = false;
  private boolean isMaxMsgsProcessed = false;
  long currentTime;
  GroupBy groupBy;
  Filter filter;
  private MessageConsumer consumer;
  private String rootDir, filterString, groupByString, toTimeString,
      fromTimeString, cuttoffString, timeOutString;
  private long maxMessages = Long.MAX_VALUE;
  private long messageCount = 0;
  private Tier cutoffTier = null;
  private String timezone;

  public AuditStatsQuery(String rootDir, String toTimeString,
      String fromTimeString, String filterString, String groupByString,
      String cuttoffTime, String timeOut, String timezone) {
    received = new TreeMap<Group, Long>();
    sent = new TreeMap<Group, Long>();
    this.rootDir = rootDir;
    this.toTimeString = toTimeString;
    this.fromTimeString = fromTimeString;
    this.filterString = filterString;
    this.groupByString = groupByString;
    this.cuttoffString = cuttoffTime;
    this.timeOutString = timeOut;
    this.timezone = timezone;
  }

  public AuditStatsQuery(String rootDir, String toTimeString,
      String fromTimeString, String filterString, String groupByString,
      String cuttoffTime, String timeOut, String timezone, Tier cutoffTier,
      long maxMessages) {
    this(rootDir, toTimeString, fromTimeString, filterString, groupByString,
        cuttoffTime, timeOut, timezone);
    this.cutoffTier = cutoffTier;
    this.maxMessages = maxMessages;
  }

  private boolean isCutoffReached(long timestamp) {
    if (messageCount >= maxMessages)
      isMaxMsgsProcessed = true;
    return (timestamp - toTime.getTime() >= cutoffTime)
        || (messageCount >= maxMessages);
  }

  void aggregateStats(MessageConsumer consumer) throws InterruptedException,
      TException, ParseException, IOException {
    Message message = null;
    TDeserializer deserialize = new TDeserializer();
    AuditMessage packet;
    currentTime = 0;

    do {
      message = consumer.next(timeout, TimeUnit.MILLISECONDS);// consumerThread.message;
      if (message == null)
        break;
      packet = new AuditMessage();
      deserialize.deserialize(packet, message.getData().array());
      LOG.debug("Packet read is " + packet);
      currentTime = packet.getTimestamp();
      Map<Column, String> values = new HashMap<Column, String>();
      values.put(Column.HOSTNAME, packet.getHostname());
      values.put(Column.TIER, packet.getTier());
      values.put(Column.TOPIC, packet.getTopic());
      if (filter.apply(values)) {
        Group group = groupBy.getGroup(values);
        Long alreadyReceived = received.get(group);
        Long alreadySent = sent.get(group);
        if (alreadyReceived == null)
          alreadyReceived = 0l;
        if (alreadySent == null)
          alreadySent = 0l;
        Long receivedCount = getSum(packet.getReceived());
        alreadyReceived += receivedCount;
        alreadySent += getSum(packet.getSent());

        if (cutoffTier != null
            && packet.getTier().equalsIgnoreCase(cutoffTier.toString())) {
          messageCount += receivedCount;
        }

        received.put(group, alreadyReceived);
        sent.put(group, alreadySent);
      }
    } while (!isCutoffReached(currentTime));
  }

  private Long getSum(Map<Long, Long> counters) {
    Long result = 0l;
    for (Entry<Long, Long> entry : counters.entrySet()) {
      long timestamp = entry.getKey();
      if (timestamp >= fromTime.getTime() && timestamp <= toTime.getTime())
        result += entry.getValue();
    }
    return result;

  }

  private Date getDate(String date) throws ParseException {
    SimpleDateFormat formatter = new SimpleDateFormat(AuditUtil.DATE_FORMAT);
    if (timezone != null)
      formatter.setTimeZone(TimeZone.getTimeZone(timezone));
    return formatter.parse(date);
  }

  public void execute() throws ParseException, IOException,
      InterruptedException, TException {
    try {
      parseAndSetArguments();
      aggregateStats(consumer);
    } finally {
      if (consumer != null)
        consumer.close();
    }
  }

  void parseAndSetArguments() throws ParseException, IOException {
    if (cuttoffString == null)
      cutoffTime = 3600000l;
    else
      cutoffTime = Long.parseLong(cuttoffString) * 60 * 1000;
    if (timeOutString == null)
      timeout = 120000;
    else
      timeout = Long.parseLong(timeOutString) * 60 * 1000;
    groupBy = new GroupBy(groupByString);
    filter = new Filter(filterString);
    fromTime = getDate(fromTimeString);
    toTime = getDate(toTimeString);
    consumer = getConsumer(fromTime, rootDir);
  }

  public static void main(String args[]) {
    String cutoffString = null, timeoutString = null;
    String groupByKeys = null;
    String filterKeys = null;
    String rootDir = null;
    String fromTime = null, toTime = null;
    String timezone = null;
    try {
      if (args.length < minArgs) {
        printUsage();
        return;
      }
      for (int i = 0; i < args.length;) {
        if (args[i].equalsIgnoreCase("-cutoff")) {
          cutoffString = args[i + 1];
          LOG.info("Cuttof Time is  " + cutoffString);
          i = i + 2;
        } else if (args[i].equalsIgnoreCase("-group")) {
          groupByKeys = args[i + 1];
          LOG.info("Group is " + groupByKeys);
          i = i + 2;
        } else if (args[i].equalsIgnoreCase("-filter")) {
          filterKeys = args[i + 1];
          LOG.info("Filter is " + filterKeys);
          i = i + 2;
        } else if (args[i].equalsIgnoreCase("-rootdir")) {
          rootDir = args[i + 1];
          i = i + 2;
        } else if (args[i].equalsIgnoreCase("-timeout")) {
          timeoutString = args[i + 1];
          i = i + 2;
        } else if (args[i].equalsIgnoreCase("-timezone")) {
          timezone = args[i + 1];
          i = i + 2;
        } else {
          if (fromTime == null) {
            fromTime = args[i++];
            LOG.info("From time is " + fromTime);
          } else {
            toTime = args[i++];
            LOG.info("To time is " + toTime);
          }
        }

      }
      if (fromTime == null || toTime == null || rootDir == null) {
        printUsage();
        System.exit(-1);
      }
      AuditStatsQuery auditStatsQuery = new AuditStatsQuery(rootDir, toTime,
          fromTime, filterKeys, groupByKeys, cutoffString, timeoutString,
          timezone);
      try {
        auditStatsQuery.execute();
      } catch (InterruptedException e) {
        LOG.error("Exception in query", e);
        System.exit(-1);
      } catch (TException e) {
        LOG.error("Exception in query", e);
        System.exit(-1);
      }
      System.out.println("Displaying results for " + auditStatsQuery);
      auditStatsQuery.displayResults();
    } catch (Throwable e) {
      LOG.error("Runtime Exception", e);
      System.exit(-1);
    }
  }

  @Override
  public String toString() {
    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM HH:mm");
    if (timezone != null)
      formatter.setTimeZone(TimeZone.getTimeZone(timezone));
    return "AuditStatsQuery [fromTime=" + formatter.format(fromTime)
        + ", toTime=" + formatter.format(toTime) + ", cutoffTime=" + cutoffTime
        + ", groupBy=" + groupBy + ", filter=" + filter + ", timeout=" + timeout 
        + ", rootdir=" + rootDir + "]";
  }

  public void displayResults() {
    if (isTimeOut) {
      System.out
          .println("Query was stopped due to timeout limit,Partial Result Possible");
      SimpleDateFormat formatter = new SimpleDateFormat();
      if (timezone != null)
        formatter.setTimeZone(TimeZone.getTimeZone(timezone));
      String date = formatter.format(new Date(currentTime));
      System.out.println("Time of Last Processed Audit Message [ " + date
          + " ]");
    } else if (isMaxMsgsProcessed) {
      System.out
          .println("Query was stopped due to maximum number of messages processed");
    }
    System.out.println("Group \t\t\t\t\t Received");
    for (Entry<Group, Long> entry : received.entrySet()) {
      System.out.println(entry.getKey() + " \t" + entry.getValue());
    }
  }

  MessageConsumer getConsumer(Date fromTime, String rootDir)
      throws IOException {
    Calendar calendar = Calendar.getInstance();
    if (timezone != null)
      calendar.setTimeZone(TimeZone.getTimeZone(timezone));
    calendar.setTime(fromTime);
    calendar.add(Calendar.HOUR_OF_DAY, -1);

    ClientConfig config = ClientConfig
        .loadFromClasspath(MESSAGE_CLIENT_CONF_FILE);
    config.set(ROOT_DIR_KEY, rootDir);
    LOG.info("Intializing pintail from " + calendar.getTime());
    return MessageConsumerFactory.create(config, calendar.getTime());
  }

  private static void printUsage() {
    StringBuffer usage = new StringBuffer();
    usage.append("Usage : AuditStatsQuery ");
    usage.append("-rootdir <hdfs root dir>");
    usage.append("[-cutoff <cuttofTimeInMins>]");
    usage.append("[-timeout <timeoutInMins>]");

    usage.append("[-group <comma seperated columns>]");
    usage.append("[-filter <comma seperated column=<value>>]");
    usage.append("where column can take value :[");
    for (Column key : Column.values()) {
      usage.append(key);
      usage.append(",");
    }
    usage.append("]");
    usage.append("fromTime(" + AuditUtil.DATE_FORMAT + ")" + "toTime("
        + AuditUtil.DATE_FORMAT + ")");
    System.out.println(usage);
  }

}
