package com.inmobi.messaging.publisher.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.EndOfStreamException;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.audit.AuditStatsQuery;
import com.inmobi.messaging.consumer.audit.Column;
import com.inmobi.messaging.consumer.audit.GroupBy;
import com.inmobi.messaging.consumer.audit.GroupBy.Group;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.util.AuditUtil;

class Counters {
  long success = 0, invocations = 0, unhandled = 0, graceful = 0, inflight = 0,
      lost = 0, reconnect = 0, retry = 0;
}

public class RandomizedMultiTopicSeqGenerator {

  public static final String[] tiers = { "publisher", "agent", "collector",
      "hdfs" };
  public static final String OVERALL_RECEIVED_GOOD =
      "scribe_overall:received good";
  public static final String AGENT_TOPIC_WRITTEN_SUFFIX =
      ":std_wrote_num_messages";
  public static final String AGENT_OVERALL_WRITTEN = "scribe_overall:sent";
  public static final String TOPIC_RECEIVED_SUFFIX = ":received good";
  public static final String COLLECTOR_OVERALL_WRITTEN =
      "scribe_overall:hdfs_wrote_num_messages";
  public static final String COLLECTOR_TOPIC_WRITTEN_SUFFIX =
      ":hdfs_wrote_num_messages";

  static void waitToComplete(AbstractMessagePublisher publisher, String topic)
      throws InterruptedException {
    if (publisher.getStats(topic) != null) {
      while (publisher.getStats(topic).getInFlight() > 0) {
        System.out.println("Inflight: "
            + publisher.getStats(topic).getInFlight());
        Thread.sleep(100);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    final int numTopics = 3;
    boolean isFail = false;
    boolean isWarn = false;
    List<AbstractMessagePublisher> publishers =
        new ArrayList<AbstractMessagePublisher>();
    long sleep;
    Date start, end;
    int agentPort, collectorPort;
    Map<String, Integer> initialScribeAgent, initialScribeCollector, finalScribeAgent, finalScribeCollector;
    if (args.length != 8) {
      System.err
          .println("Usage: RandomizedMultiTopicSeqGenerator <topic1> <topic2>"
              + "<num_of_publishers> <maxSeqPerThread> <number_of_threads> <agentPort> <collectorPort> <sleepDurationInMillis>");
      return;
    }

    String topics[] = new String[numTopics];
    topics[0] = args[0];
    topics[1] = args[1];
    topics[2] = AuditUtil.AUDIT_STREAM_TOPIC_NAME;
    int numPublishers = Integer.parseInt(args[2]);
    long maxSeq = Long.parseLong(args[3]);
    int totalThread = Integer.parseInt(args[4]);
    agentPort = Integer.parseInt(args[5]);
    collectorPort = Integer.parseInt(args[6]);
    sleep = Long.parseLong(args[7]);
    long totalMsgs = maxSeq * totalThread;

    StringBuffer failureReason = new StringBuffer();
    for (int i = 0; i < numPublishers; i++) {
      publishers.add((AbstractMessagePublisher) MessagePublisherFactory
          .create());
    }

    PublishThreadNew[] p = new PublishThreadNew[totalThread];
    CountDownLatch latch = new CountDownLatch(totalThread);
    start = new Date();
    System.out.println("Starting at " + start);
    initialScribeAgent = executeScibeCtrl(agentPort);
    initialScribeCollector = executeScibeCtrl(collectorPort);
    for (int thread = 0; thread < totalThread; thread++) {
      p[thread] =
          new PublishThreadNew(topics[0], topics[1], publishers, maxSeq, latch);
      p[thread].start();
    }
    latch.await();
    int i = 0;

    Counters total_counters[] = new Counters[numTopics];
    for (int j = 0; j < total_counters.length; j++) {
      total_counters[j] = new Counters();
    }
    for (AbstractMessagePublisher publisher : publishers) {
      publisher.close();
      i++;
      Counters counters[] = new Counters[numTopics];
      for (int j = 0; j < counters.length; j++) {
        counters[j] = new Counters();
        TimingAccumulator accum = publisher.getStats(topics[j]);
        if (accum != null) {
          counters[j].invocations = accum.getInvocationCount();
          System.out.println("Total invocations for topic  " + topics[j]
              + " by publisher " + i + " [" + counters[j].invocations + "]");
          total_counters[j].invocations += counters[j].invocations;
          counters[j].success = accum.getSuccessCount();
          System.out.println("Total Success for topic  " + topics[j]
              + " by publisher " + i + " [" + counters[j].success + "]");
          total_counters[j].success += counters[j].success;
          counters[j].unhandled = accum.getUnhandledExceptionCount();
          System.out.println("Total Unhandled Exceptions for topic  "
              + topics[j] + " by publisher " + i + " [" + counters[j].unhandled
              + "]");
          total_counters[j].unhandled += counters[j].unhandled;
          counters[j].graceful = accum.getGracefulTerminates();
          System.out.println("Total GraceFul Shutdowns for topic  " + topics[j]
              + " by publisher " + i + " [" + counters[j].graceful + "]");
          total_counters[j].graceful += counters[j].graceful;
          counters[j].inflight = accum.getInFlight();
          System.out.println("Total  InFlight for  " + topics[j]
              + " by publisher " + i + " [" + counters[j].inflight + "]");
          total_counters[j].inflight += counters[j].inflight;
          counters[j].lost = accum.getLostCount();
          System.out.println("Total lost for topic  " + topics[j]
              + " by publisher " + i + " [" + counters[j].lost + "]");
          total_counters[j].lost += counters[j].lost;
          counters[j].reconnect = accum.getReconnectionCount();
          System.out.println("Total reconnect for topic  " + topics[j]
              + " by publisher " + i + " [" + counters[j].reconnect + "]");
          total_counters[j].reconnect += counters[j].reconnect;
          counters[j].retry = accum.getRetryCount();
          System.out.println("Total retry for topic  " + topics[j]
              + " by publisher " + i + " [" + counters[j].retry + "]");
          total_counters[j].retry += counters[j].retry;
        }
      }
      System.out.println("===========================");
    }

    System.out
        .println("<===============TOTALS FOR TOPICS EXCEPT AUDIT==================================>");
    Counters sum = new Counters();
    sum.graceful = total_counters[0].graceful + total_counters[1].graceful;
    sum.inflight = total_counters[0].inflight + total_counters[1].inflight;
    sum.invocations =
        total_counters[0].invocations + total_counters[1].invocations;
    sum.lost = total_counters[0].lost + total_counters[1].lost;
    sum.reconnect = total_counters[0].reconnect + total_counters[1].reconnect;
    sum.retry = total_counters[0].retry + total_counters[1].retry;
    sum.success = total_counters[0].success + total_counters[1].success;
    sum.unhandled = total_counters[0].unhandled + total_counters[1].unhandled;
    System.out.println("Total invocations [" + sum.invocations + "]");
    System.out.println("Total Success  [" + sum.success + "]");
    System.out.println("Total unhandledExceptions[" + sum.unhandled + "]");
    System.out.println("Total graceful Shutdowns[" + sum.graceful + "]");
    System.out.println("Total inflight[" + sum.inflight + "]");
    System.out.println("Total Lost[" + sum.lost + "]");
    System.out.println("Total Retry[" + sum.retry + "]");
    System.out.println("Total Reconnect[" + sum.reconnect + "]");
    /* ######################### VALIDATION ############## */
    end = new Date();
    System.out.println("Sleeping at " + end);
    Thread.sleep(sleep);

    finalScribeCollector = executeScibeCtrl(collectorPort);
    finalScribeAgent = executeScibeCtrl(agentPort);
    System.out.println("Initial scribe counters of Agent \n"
        + initialScribeAgent);
    System.out.println("Final Scribe Counters of Agent \n" + finalScribeAgent);
    System.out.println("Initial scribe counters of Collector \n"
        + initialScribeCollector);
    System.out.println("Final Scribe counters of collector \n"
        + finalScribeCollector);

    if (sum.graceful != 0 || sum.lost != 0 || sum.reconnect != 0
        || sum.retry != 0 || sum.unhandled != 0)
      isWarn = true;
    isFail =
        validateCounters(start, end, topics, totalMsgs, failureReason, sum,
            total_counters);
    isFail =
        validateScribeCounters(initialScribeAgent, initialScribeCollector,
            finalScribeAgent, finalScribeCollector, topics, failureReason, sum,
            total_counters)
            || isFail;
    isFail =
        validateHDFSCount(start, topics, total_counters, failureReason)
            || isFail;
    if (isFail) {
      System.out.println("VALIDATION FAILED");
      System.out.println("REASON " + failureReason);
    } else {
      System.out.println("VALIDATION SUCESS");
    }
    if (isWarn) {
      System.out.println("WARNING");
      System.out.println("One of counters is not 0 but was expected to be 0");
    }

  }

  private static boolean validateScribeCounters(
      Map<String, Integer> initialScribeAgent,
      Map<String, Integer> initialScribeCollector,
      Map<String, Integer> finalScribeAgent,
      Map<String, Integer> finalScribeCollector, String[] topics,
      StringBuffer failureReason, Counters sum, Counters[] total_counters) {
    boolean isFail = false;

    Integer agentRec[] = new Integer[2];
    agentRec[0] =
        finalScribeAgent.get(topics[0] + TOPIC_RECEIVED_SUFFIX)
            - initialScribeAgent.get(topics[0] + TOPIC_RECEIVED_SUFFIX);
    agentRec[1] =
        finalScribeAgent.get(topics[1] + TOPIC_RECEIVED_SUFFIX)
            - initialScribeAgent.get(topics[1] + TOPIC_RECEIVED_SUFFIX);
    if (agentRec[0] != total_counters[0].success) {
      isFail = true;
      failureReason
          .append("Agent received doesn't match success of publisher for topic ["
              + topics[0] + "];found [" + agentRec[0] + "]");

    }
    if (agentRec[1] != total_counters[1].success) {
      isFail = true;
      failureReason
          .append("Agent received doesn't match success of publisher for topic ["
              + topics[1] + "];found [" + agentRec[1] + "]");

    }

    Integer collectorRec[] = new Integer[2];
    collectorRec[0] =
        finalScribeCollector.get(topics[0] + TOPIC_RECEIVED_SUFFIX)
            - initialScribeCollector.get(topics[0] + TOPIC_RECEIVED_SUFFIX);
    collectorRec[1] =
        finalScribeCollector.get(topics[1] + TOPIC_RECEIVED_SUFFIX)
            - initialScribeCollector.get(topics[1] + TOPIC_RECEIVED_SUFFIX);
    if (collectorRec[0] != total_counters[0].success) {
      isFail = true;
      failureReason
          .append("Collector received doesn't match success of publisher for topic ["
              + topics[0] + "];found [" + collectorRec[0] + "]");

    }
    if (collectorRec[1] != total_counters[1].success) {
      isFail = true;
      failureReason
          .append("Collector received doesn't match success of publisher for topic ["
              + topics[1] + "];found [" + collectorRec[1] + "]");

    }
    return isFail;
  }

  private static Map<String, Integer> executeScibeCtrl(int port)
      throws IOException {
    Map<String, Integer> returnValue = new HashMap<String, Integer>();
    Process process =
        Runtime.getRuntime().exec("scribe_ctrl.py counters " + port);
    InputStream inStream = process.getInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    String read = reader.readLine();
    while (read != null) {
      int index = read.lastIndexOf(":");
      if (index != -1) {
        returnValue.put(read.substring(0, index),
            Integer.parseInt(read.substring(index + 1).trim()));
      }
      read = reader.readLine();
    }
    return returnValue;

  }

  private static boolean validateHDFSCount(Date start, String topics[],
      Counters[] total_counters, StringBuffer failureReason)
      throws IOException, InterruptedException, EndOfStreamException {
    boolean isFail = false;
    ClientConfig config =
        ClientConfig
            .loadFromClasspath(MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);

    long[] count = new long[topics.length];
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(start);
    calendar.add(Calendar.MINUTE, -1);
    start = calendar.getTime();
    for (int i = 0; i < topics.length - 1; i++) {
      config.set(MessageConsumerFactory.TOPIC_NAME_KEY, topics[i]);
      MessageConsumer consumer = MessageConsumerFactory.create(config, start);
      count[i] = 0;
      while (consumer.next(1, TimeUnit.MINUTES) != null) {
        count[i]++;
      }
      consumer.close();
      if (count[i] != total_counters[i].success) {
        isFail = true;
        failureReason.append("Total number of messages in hdfs is [" + count[i]
            + "] expected is [" + total_counters[i].success + "] \n");
      }
    }
    return isFail;
  }

  private static boolean validateCounters(Date start, Date end,
      String[] topics, long totalMsgs, StringBuffer failureReason,
      Counters sum, Counters[] total_counters) throws ParseException,
      IOException, InterruptedException, TException, EndOfStreamException {
    boolean isFail = false;
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(end);
    calendar.add(Calendar.MINUTE, 1);
    end = calendar.getTime();
    SimpleDateFormat formatter = new SimpleDateFormat(AuditUtil.DATE_FORMAT);
    AuditStatsQuery query =
        new AuditStatsQuery(null, formatter.format(end),
            formatter.format(start), null, "topic,tier", "10", "1", null);
    query.execute();
    Map<Group, Long> received = query.getReceived();
    System.out.println("RECEIVED FROM QUERY " + received);
    GroupBy groupby = new GroupBy("tier,topic");
    Long[] receivedCount = new Long[tiers.length * topics.length];
    int k = 0;
    for (String tier : tiers) {
      for (int z = 0; z < topics.length - 1; z++) {
        Map<Column, String> grpValues = new HashMap<Column, String>();
        grpValues.put(Column.TIER, tier);
        grpValues.put(Column.TOPIC, topics[z]);
        receivedCount[k] = received.get(groupby.getGroup(grpValues));
        if (receivedCount[k] == null
            || receivedCount[k] != total_counters[z].success) {
          isFail = true;
          failureReason
              .append("Invocation count doesn't match with the query result for topic = ["
                  + topics[z] + "] and tier = [" + tier + "] \n");
        }
        k++;
      }
    }
    long count = 0;
    if (receivedCount[0] != null)
      count += receivedCount[0];
    if (receivedCount[2] != null)
      count += receivedCount[1];
    if (count != totalMsgs) {
      isFail = true;
      failureReason.append("Expected count of messages accross topics is"
          + totalMsgs + " found total is " + count + "\n");
    }
    if (sum.invocations != count) {
      isFail = true;
      failureReason
          .append("Total Invocations doesn't match the query result;expected ["
              + sum.invocations + "]  found [" + count + "] \n");
    }
    if (sum.invocations != sum.success) {
      isFail = true;
      failureReason
          .append("Total Invocations doesn't match total success ;expected ["
              + sum.invocations + "]  found [" + sum.success + "] \n");
    }
    if (total_counters[0].invocations != total_counters[0].success
        || total_counters[1].invocations != total_counters[1].success
        || total_counters[2].invocations != total_counters[2].success) {
      isFail = true;
      failureReason
          .append("Invocations and success of individual topics doesn't match \n");

    }
    if (total_counters[0].inflight != 0 || total_counters[1].inflight != 0
        || total_counters[2].inflight != 0) {
      isFail = true;
      failureReason
          .append("Even after pubsliher is closed there are some inflight messages");
    }
    return isFail;
  }

  private static class PublishThreadNew extends Thread {
    private String topics[];

    private List<AbstractMessagePublisher> publishers;
    private long maxSeq;
    private CountDownLatch latch;
    Random random;

    PublishThreadNew(String topic1, String topic2,
        List<AbstractMessagePublisher> publishers, long maxSeq,
        CountDownLatch latch) {
      topics = new String[2];
      topics[0] = topic1;
      topics[1] = topic2;
      this.publishers = publishers;
      this.maxSeq = maxSeq;
      this.latch = latch;
      random = new Random();
    }

    private
        void publishMessages(AbstractMessagePublisher publisher, long maxSeq)
            throws InterruptedException {
      for (long seq = 1; seq <= maxSeq; seq++) {
        Message msg =
            new Message(ByteBuffer.wrap(Long.toString(seq).getBytes()));
        publisher.publish(topics[random.nextInt(2)], msg);
        Thread.sleep(random.nextInt(5) * 100);
      }
    }

    public void run() {
      try {

        publishMessages(publishers.get(random.nextInt(publishers.size())),
            maxSeq);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        latch.countDown();
      }
    }
  }

}
