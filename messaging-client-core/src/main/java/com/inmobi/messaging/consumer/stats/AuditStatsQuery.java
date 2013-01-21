package com.inmobi.messaging.consumer.stats;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.stats.GroupBy.Group;

public class AuditStatsQuery {

  // each map will have total aggregate for all different keys provided in the
  // query
  Map<Group, Long> received;
  Map<Group, Long> sent;
  private static final int minArgs = 4;
  private static final Logger LOG = LoggerFactory
      .getLogger(AuditStatsQuery.class);
  private static final String TIER = "tier";
  private static final String HOSTNAME = "hostname";
  private static final String TOPIC = "topic";

  private Date fromTime;
  private Date toTime;
  private long cutoffTime;
  private GroupBy groupBy;
  private Filter filter;
  private AuditStatsQuery() {
    received = new HashMap<Group, Long>();
    sent = new HashMap<Group, Long>();
  }

  private boolean isCutoffReached(long timestamp) {
    return timestamp - toTime.getTime() >= cutoffTime;
  }



  private void aggregateStats(MessageConsumer consumer)
      throws InterruptedException, TException {
    Message message;
    TDeserializer deserialize = new TDeserializer();
    AuditMessage packet;
    long currentTime;
    do {
      System.out.println("READING NEXT PACKET");
      message = consumer.next();
      System.out.println("READ NEXT PACKET");
      packet = new AuditMessage();
      deserialize.deserialize(packet, message.getData().array());
      currentTime = packet.getTimestamp();
      if(filter.apply(packet)){
        Group group = groupBy.getGroup(packet.getTier(), packet.getHostname(), packet.getTopic());
        Long alreadyReceived = received.get(group);
        Long alreadySent = sent.get(group);
        if(alreadyReceived==null)
          alreadyReceived=0l;
        alreadyReceived += getSum(packet.getReceived());
        alreadySent += getSum(packet.getSent());
        received.put(group, alreadyReceived);
        sent.put(group, alreadySent);
      }
    } while (!isCutoffReached(currentTime));
  }

  private Long getSum(Map<Long, Long> counters) {
    Long result = 0l;
    for (Long counter : counters.values())
      result += counter;
    return result;

  }

  private static Date getDate(String date) throws ParseException {
    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm");
    return formatter.parse(date);
  }

  private  Filter getFilter(String keys){
    String tier=null,hostname=null,topic=null;
    if(keys ==null)
      return new Filter(null, null, null, fromTime, toTime);
    String filters[] = keys.split(",");
    for(int i=0;i<filters.length;i++){
      String tmp = filters[i];
      String keyValues[]=tmp.split("=");
      if(keyValues.length!=2)
        continue; //skip this filter as it is malformed
      if (keyValues[0] == TIER) {
        tier = stripQuotes(keyValues[0]);
      } else if (keyValues[0] == HOSTNAME) {
        hostname = stripQuotes(keyValues[0]);
      } else if (keyValues[0] == TOPIC) {
        topic = stripQuotes(keyValues[0]);
      }
    }
    return new Filter(tier, hostname, topic, fromTime, toTime);
  }

  private static String stripQuotes(String input) {
    if (input.startsWith("'") || input.startsWith("\""))
      input = input.substring(1);
    if (input.endsWith("'") || input.endsWith("\""))
      input = input.substring(0, input.length() - 1);
    return input;
  }
  public static void main(String args[]) {
    String confFile = null;
    AuditStatsQuery statsQuery = new AuditStatsQuery();
    String groupByKeys = null;
    String filterKeys = null;
    // TODO read cmd line options
    if (args.length < minArgs) {
      printUsage();
      return;
    }
    for (int i = 0; i < args.length;) {
      if (args[i].equalsIgnoreCase("-conf")) {
        confFile = args[i + 1];
        LOG.info("Conf file is " + confFile);
        i = i + 2;
      } else if (args[i].equalsIgnoreCase("-group")) {
        groupByKeys = args[i + 1];
        LOG.info("Group is " + groupByKeys);
        i = i + 2;
      } else if (args[i].equalsIgnoreCase("-filter")) {
        filterKeys = args[i + 1];
        LOG.info("Filter is " + filterKeys);
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
    ClientConfig config = ClientConfig.load(confFile);
    statsQuery.cutoffTime = config.getLong("cuttoffTime", 60000000l);
    statsQuery.groupBy = new GroupBy(groupByKeys);
    statsQuery.filter = statsQuery.getFilter(filterKeys);
    MessageConsumer consumer = null;
    // statsQuery.keys = keys.split(",");
    try {
      consumer = getConsumer(confFile, statsQuery.fromTime);
      statsQuery.aggregateStats(consumer);
      statsQuery.displayResults();
    } catch (IOException e) {
      LOG.error("Check the config file");
    } catch (InterruptedException e) {
      LOG.error("Consumer was interrupted while aggregating stats", e);
    } catch (TException e) {
      LOG.error("Corrupted packet data;exiting", e);
    }

  }

  private void displayResults() {
    System.out.println("Group \t Received \t Sent \t");
    for (Entry<Group, Long> entry : received.entrySet()) {
      System.out.println(entry.getKey() + " \t" + entry.getValue() + " \t"
          + sent.get(entry.getKey()));
    }
  }
  private static MessageConsumer getConsumer(String confFile, Date fromTime)
      throws IOException {
    return MessageConsumerFactory.create(confFile, fromTime);
  }

  private static void printUsage() {
    System.out
        .println("Usage : AuditStatsQuery -conf <conf file path> [-group tier,hostname,topic] [-filter tier='abc',hostname='xyz',topic='ABC'] fromTime(dd-mm-yyyy HH:mm) toTime(dd-mm-yyyy HH:mm");
  }

}
