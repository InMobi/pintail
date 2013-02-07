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

enum Columns {
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
  private boolean isPartialResult =false;
  long currentTime;
  GroupBy groupBy;
  Filter filter;

  AuditStatsQuery() {
    received = new HashMap<Group, Long>();
    sent = new HashMap<Group, Long>();
  }

  class ConsumerWorker extends Thread {
    private Message message = null;
    private MessageConsumer consumer;

    ConsumerWorker(MessageConsumer consumer) {
      this.consumer = consumer;
    }

    @Override
    public void run() {
      try {
        message = consumer.next();
      } catch (InterruptedException e) {
        LOG.debug("Consumer Thread interuppted", e);
        isPartialResult=true;
      }

    }

  }

  private boolean isCutoffReached(long timestamp) {
    return timestamp - toTime.getTime() >= cutoffTime;
  }



  void aggregateStats(MessageConsumer consumer)
      throws InterruptedException, TException {
    Message message = null;
    TDeserializer deserialize = new TDeserializer();
    AuditMessage packet;
    currentTime = 0;
    do {
      ConsumerWorker consumerThread = new ConsumerWorker(consumer);
      consumerThread.start();
      consumerThread.join(timeout);
      if (consumerThread.message == null) {
        consumerThread.interrupt();
        break;
      }
      message = consumerThread.message;
      packet = new AuditMessage();
      deserialize.deserialize(packet, message.getData().array());
      LOG.debug("Packet read is " + packet);
      currentTime = packet.getTimestamp();
      Map<Columns, String> values = new HashMap<Columns, String>();
      values.put(Columns.HOSTNAME, packet.getHostname());
      values.put(Columns.TIER, packet.getTier());
      values.put(Columns.TOPIC, packet.getTopic());
      if (filter.apply(values)) {
        Group group = groupBy.getGroup(values);
        Long alreadyReceived = received.get(group);
        Long alreadySent = sent.get(group);
        if(alreadyReceived==null)
          alreadyReceived=0l;
        if (alreadySent == null)
          alreadySent = 0l;
        alreadyReceived += getSum(packet.getReceived());
        alreadySent += getSum(packet.getSent());
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

  private static Date getDate(String date) throws ParseException {
    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy-HH:mm");
    formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    return formatter.parse(date);
  }

  public static void main(String args[]) {
    String cutoff = null;
    AuditStatsQuery statsQuery = new AuditStatsQuery();
    String groupByKeys = null;
    String filterKeys = null;
    String rootDir = null;
    if (args.length < minArgs) {
      printUsage();
      return;
    }
    for (int i = 0; i < args.length;) {
      if (args[i].equalsIgnoreCase("-cutoff")) {
        cutoff = args[i + 1];
        LOG.info("Cuttof Time is  " + cutoff);
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
        statsQuery.timeout = Long.parseLong(args[i + 1]) * 60 * 1000;
        i = i + 2;
      } else {
        try {
          if (statsQuery.fromTime == null) {
            statsQuery.fromTime = getDate(args[i++]);
            LOG.info("From time is " + statsQuery.fromTime);
          } else {
            statsQuery.toTime = getDate(args[i++]);
            LOG.info("To time is " + statsQuery.toTime);
          }
        } catch (ParseException e) {
          printUsage();
          return;
        }
      }

    }
    if (cutoff == null || statsQuery.fromTime == null
        || statsQuery.toTime == null) {
      statsQuery.cutoffTime = 60000000l;
    } else {
      statsQuery.cutoffTime = Long.parseLong(cutoff) * 60 * 1000;
    }
    if (rootDir == null) {
      printUsage();
      return;
    }
    statsQuery.groupBy = new GroupBy(groupByKeys);
    statsQuery.filter = new Filter(filterKeys);
    MessageConsumer consumer = null;
    // statsQuery.keys = keys.split(",");
    try {
      consumer = getConsumer(statsQuery.fromTime, rootDir);
      statsQuery.aggregateStats(consumer);
      consumer.close();
      System.out.println("Displaying results for " + statsQuery);
      statsQuery.displayResults();
    } catch (IOException e) {
      LOG.error("Check the config file");
    } catch (InterruptedException e) {
      LOG.error("Consumer was interrupted while aggregating stats", e);
    } catch (TException e) {
      LOG.error("Corrupted packet data;exiting", e);
    }

  }

  @Override
  public String toString() {
    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM HH:mm");
    formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    return "AuditStatsQuery [fromTime=" + formatter.format(fromTime)
        + ", toTime=" + formatter.format(toTime)
        + ", cutoffTime=" + cutoffTime + ", groupBy=" + groupBy + ", filter="
        + filter + "]";
  }

  private void displayResults() {
    if (isPartialResult) {
      System.out
          .println("Query was stopped due to timeout limit,Partial Result Possible");
      SimpleDateFormat formatter = new SimpleDateFormat();
      formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
      String date = formatter.format(new Date(currentTime));
      System.out.println("Time of Last Processed Audit Message [ " + date
          + " ]");
    }
    System.out.println("Group \t Received \t Sent \t");
    for (Entry<Group, Long> entry : received.entrySet()) {
      System.out.println(entry.getKey() + " \t" + entry.getValue() + " \t"
          + sent.get(entry.getKey()));
    }
  }

  static MessageConsumer getConsumer(Date fromTime, String rootDir)
      throws IOException {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
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

    usage.append("[-group <");
    for (Columns key : Columns.values()) {
      usage.append(key);
      usage.append(",");
    }
    usage.append(">]");
    usage.append("[-filter tier='abc',hostname='xyz',topic='ABC']");
    usage.append("fromTime(dd-mm-yyyy-HH:mm) toTime(dd-mm-yyyy-HH:mm)");
    System.out.println(usage);
  }

}
