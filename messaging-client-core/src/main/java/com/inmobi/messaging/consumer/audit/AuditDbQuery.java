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

  Map<Tuple, Map<Float, Integer>> percentile;
  Date fromTime;
  Date toTime;
  GroupBy groupBy;
  Filter filter;
  Set<Float> percentileSet;
  Set<Tuple> tupleSet;
  Map<GroupBy.Group, Long> received;
  Map<GroupBy.Group, Long> sent;

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
    tupleSet = new HashSet<Tuple>();
    this.rootDir = rootDir;
    this.toTimeString = toTimeString;
    this.fromTimeString = fromTimeString;
    this.filterString = filterString;
    this.groupByString = groupByString;
    this.percentileString = percentileString;
    this.dbConfFile = dbConfFile;
  }

  void aggregateStats() {
    tupleSet.addAll(
        AuditDBHelper.retrieve(toTime, fromTime, filter, groupBy, dbConfFile));
    setReceivedAndSentStats();
    if (percentileSet.size() > 0) {
      LOG.debug("Percentile set not empty..Creating percentile map for all " +
          "tuples;");
      populatePercentileMap();
    }
  }

  private void setReceivedAndSentStats() {
    for(Tuple tuple : tupleSet) {
      if (!tuple.isGroupBySet())
        tuple.setGroupBy(groupBy);
      GroupBy.Group group = tuple.getGroup();
      received.put(group, tuple.getReceived());
      sent.put(group, tuple.getSent());
    }
  }

  private void populatePercentileMap() {
    for (Tuple tuple : tupleSet) {
      LOG.debug("Creating percentile map for tuple :"+tuple.toString());
      Long totalCount = tuple.getReceived() - tuple.getLostCount();
      Long currentCount = 0l;
      Iterator<Float> it = percentileSet.iterator();
      Float currentPercentile = 0.0f;
      if(it.hasNext())
        currentPercentile = it.next();
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        if (latencyColumn == LatencyColumns.C600)
          continue;
        Long value = tuple.getLatencyCountMap().get(latencyColumn);
        if (currentCount + value >=
            ((currentPercentile * totalCount) / 100)) {
          Map<Float, Integer> percentileMap = percentile.get(tuple);
          if (percentileMap == null)
            percentileMap = new HashMap<Float, Integer>();
          percentileMap.put(currentPercentile, latencyColumn.getValue());
          percentile.put(tuple, percentileMap);
          if(it.hasNext())
            currentPercentile = it.next();
        }
        currentCount += value;
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
    percentileSet = getPercentileList(percentileString);
  }

  private Set<Float> getPercentileList(String percentileString) {
    if (percentileString != null || !percentileString.isEmpty()) {
      Set<Float> percentileSet = new TreeSet<Float>();
      String[] percentiles = percentileString.split(",");
      for (String percentile : percentiles)
        percentileSet.add(Float.parseFloat(percentile));
      return percentileSet;
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
    for (Tuple tuple : tupleSet) {
      results.append(tuple.getGroup()+"\t");
      results.append(received.get(tuple.getGroup())+"\t");
      Map<Float, Integer> percentileMap = percentile.get(tuple);
      for (Map.Entry<Float, Integer> percentileEntry : percentileMap.entrySet()) {
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

  public Map<Tuple, Map<Float, Integer>> getPercentile() {
    return percentile;
  }
}

