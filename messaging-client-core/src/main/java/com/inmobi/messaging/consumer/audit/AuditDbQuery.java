package com.inmobi.messaging.consumer.audit;

import com.inmobi.messaging.util.AuditDBHelper;
import com.inmobi.messaging.util.AuditUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class AuditDbQuery {

  private static final int minArgs = 2;
  private static final Logger LOG =
      LoggerFactory.getLogger(AuditDbQuery.class);

  private String rootDir, filterString, groupByString, toTimeString,
      fromTimeString, percentileString, dbConfFile;

  Map<GroupBy.Group, Long> received;
  Map<GroupBy.Group, Long> sent;
  Map<GroupBy.Group, Map<LatencyColumns, Long>> latencyCount;
  Map<GroupBy.Group, Map<Float, Float>> percentile;
  Date fromTime;
  Date toTime;
  GroupBy groupBy;
  Filter filter;
  Set<Float> percentileList;

  public AuditDbQuery(String rootDir, String toTimeString,
                         String fromTimeString, String filterString,
                         String groupByString, String percentile) {
    this(rootDir, toTimeString, fromTimeString, filterString, groupByString,
        percentile, null);
  }

  public AuditDbQuery(String rootDir, String toTimeString,
                         String fromTimeString, String filterString,
                         String groupByString) {
    this(rootDir, toTimeString, fromTimeString, filterString, groupByString,
        null, null);
  }

  public AuditDbQuery(String rootDir, String toTimeString,
                         String fromTimeString, String filterString,
                         String groupByString, String percentileString,
                         String dbConfFile) {
    received = new TreeMap<GroupBy.Group, Long>();
    sent = new TreeMap<GroupBy.Group, Long>();
    latencyCount = new TreeMap<GroupBy.Group, Map<LatencyColumns, Long>>();
    this.rootDir = rootDir;
    this.toTimeString = toTimeString;
    this.fromTimeString = fromTimeString;
    this.filterString = filterString;
    this.groupByString = groupByString;
    this.percentileString = percentileString;
    this.dbConfFile = dbConfFile;
  }

  void aggregateStats() {
    Set<Tuple> tupleSet =
        AuditDBHelper.retrieve(toTime, fromTime, filter, groupBy, dbConfFile);
    for (Tuple tuple : tupleSet) {
      LOG.debug("Aggregating stats on tuple :"+tuple.toString());
      Map<Column, String> values = new HashMap<Column, String>();
      values.put(Column.HOSTNAME, tuple.getHostname());
      values.put(Column.TIER, tuple.getTier());
      values.put(Column.TOPIC, tuple.getTopic());
      values.put(Column.CLUSTER, tuple.getCluster());
      GroupBy.Group group = groupBy.getGroup(values);
      Map<LatencyColumns, Long> latencyCountMap = new HashMap();
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        Long currentCount = tuple.getLatencyCountMap().get(latencyColumn);
        latencyCountMap.put(latencyColumn, currentCount);
      }
      received.put(group, tuple.getReceived());
      sent.put(group, tuple.getSent());
      latencyCount.put(group, latencyCountMap);
    }
    ;
    if (percentileList.size() > 0) {
      populatePercentileMap();
    }
  }

  private void populatePercentileMap() {
    for (Map.Entry<GroupBy.Group, Map<LatencyColumns, Long>> entry : latencyCount
        .entrySet()) {
      Map<LatencyColumns, Long> latencyCountMap = entry.getValue();
      GroupBy.Group group = entry.getKey();
      Long totalCount = received.get(group);
      Long currentCount = 0l;
      Long weightedSum = 0l;
      Iterator<Float> it = percentileList.iterator();
      Float currentPercentile = it.next();
      for (Map.Entry<LatencyColumns, Long> countEntry : latencyCountMap
          .entrySet()) {
        if (currentCount + countEntry.getValue() <
            ((currentPercentile * totalCount) / 100)) {
          currentCount += countEntry.getValue();
          weightedSum +=
              (countEntry.getKey().getValue() * countEntry.getValue());
        } else {
          Long diffCount =(long)
              ((currentPercentile * totalCount) / 100) - currentCount;
          currentCount += diffCount;
          weightedSum += (diffCount * countEntry.getKey().getValue());
          Map<Float, Float> percentileMap = percentile.get(group);
          if (percentileMap == null)
            percentileMap = new HashMap<Float, Float>();
          percentileMap.put(currentPercentile,
              Float.valueOf(weightedSum / currentCount));
          currentPercentile = it.next();
          currentCount += countEntry.getValue() - diffCount;
          weightedSum += ((countEntry.getValue() - diffCount) *
              countEntry.getKey().getValue());
        }
      }
    }
  }

  private Date getDate(String date) throws ParseException {
    SimpleDateFormat formatter = new SimpleDateFormat(AuditUtil.DATE_FORMAT);
    return formatter.parse(date);
  }

  public void execute()
      throws ParseException, IOException, InterruptedException, TException {
    parseAndSetArguments();
    aggregateStats();
  }

  void parseAndSetArguments() throws ParseException, IOException {
    groupBy = new GroupBy(groupByString);
    filter = new Filter(filterString);
    fromTime = getDate(fromTimeString);
    toTime = getDate(toTimeString);
    percentileList = getPercentileList(percentileString);
  }

  private Set<Float> getPercentileList(String percentileString) {
    if (percentileString != null || !percentileString.isEmpty()) {
      Set<Float> percentileList = new TreeSet<Float>();
      String[] percentiles = percentileString.split(",");
      for (String percentile : percentiles)
        percentileList.add(Float.parseFloat(percentile));
      return percentileList;
    }
    return null;
  }

  public static void main(String args[]) {
    String groupByKeys = null;
    String filterKeys = null;
    String rootDir = null;
    String fromTime = null, toTime = null;
    String percentileString = null;
    try {
      if (args.length < minArgs) {
        printUsage();
        return;
      }
      for (int i = 0; i < args.length; ) {
        if (args[i].equalsIgnoreCase("-group")) {
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
        } else if (args[i].equalsIgnoreCase("-percentile")) {
          percentileString = args[i + 1];
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
      if (fromTime == null || toTime == null) {
        printUsage();
        System.exit(-1);
      }
      AuditDbQuery auditQuery =
          new AuditDbQuery(rootDir, toTime, fromTime, filterKeys,
              groupByKeys, percentileString);
      try {
        auditQuery.execute();
      } catch (InterruptedException e) {
        LOG.error("Exception in query", e);
        System.exit(-1);
      } catch (TException e) {
        LOG.error("Exception in query", e);
        System.exit(-1);
      }
      System.out.println("Displaying results for " + auditQuery);
      auditQuery.displayResults();
    } catch (Throwable e) {
      LOG.error("Runtime Exception", e);
      System.exit(-1);
    }
  }

  @Override
  public String toString() {
    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM HH:mm");
    return "AuditStatsQuery [fromTime=" + formatter.format(fromTime) +
        ", toTime=" + formatter.format(toTime) + ", groupBy=" + groupBy +
        ", filter=" + filter + ", rootdir=" + rootDir + ", " +
        "percentiles=" + percentileString + "]";
  }

  public void displayResults() {
    StringBuffer results = new StringBuffer();
    results.append("Group \t\t\tReceived\t\t\t<Percentile, Latency>");
    for (Map.Entry<GroupBy.Group, Long> entry : received.entrySet()) {
      results.append(entry.getKey()+"\t");
      results.append(entry.getValue()+"\t");
      Map<Float, Float> percentileMap = percentile.get(entry.getKey());
      for (Map.Entry<Float, Float> percentileEntry : percentileMap.entrySet()) {
        results.append("<"+percentileEntry.getKey()+",\t");
        results.append(percentileEntry.getValue()+">\t");
      }
      results.append("\n");
    }
    System.out.println(results);
  }

  private static void printUsage() {
    StringBuffer usage = new StringBuffer();
    usage.append("Usage : AuditDbQuery ");
    usage.append("[-rootdir <hdfs root dir>]");
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
    usage.append("[-percentile <comma seperated percentile>]");
    usage.append("fromTime(" + AuditUtil.DATE_FORMAT + ")" + "toTime(" +
        AuditUtil.DATE_FORMAT + ")");
    System.out.println(usage);
  }

  public Map<GroupBy.Group, Long> getReceived() {
    return received;
  }

  public Map<GroupBy.Group, Long> getSent() {
    return sent;
  }

  public Map<GroupBy.Group, Map<Float, Float>> getPercentile() {
    return percentile;
  }
}

