package com.inmobi.messaging.consumer.audit;

import java.io.IOException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MockInMemoryConsumer;
import com.inmobi.messaging.consumer.audit.GroupBy.Group;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.publisher.MockInMemoryPublisher;

public class TestAuditStatsQuery {
  private MessagePublisher publisher;
  private Date startTime, endTime;

  private int totalData = 2;
  String topic = "topic1";
  String topic1 = "topic2";
  String topic2 = "topic3";
  String topic3 = "topic4";
  @BeforeTest
  public void setup() throws IOException {
    publisher = MessagePublisherFactory
        .create("src/test/resources/mock-publisher.properties");

  }




  private void generateData(MessagePublisher publisher, String topic,
      String topic1) {
    String msg = "sample data";

    startTime = new Date();
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(startTime);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    startTime = calendar.getTime();
    for (int i = 0; i < totalData / 2; i++) {
      publisher.publish(topic, new Message(msg.getBytes()));
    }
    for (int i = 0; i < totalData / 2; i++) {
      publisher.publish(topic1, new Message(msg.getBytes()));
    }
    endTime = new Date();
  }

  private void generateData(String topic, String topic1) {
    generateData(publisher, topic, topic1);
  }

  @Test
  public void testAuditQuery() throws IOException, InterruptedException,
      TException {
    assert (publisher instanceof com.inmobi.messaging.publisher.MockInMemoryPublisher);
    generateData(topic, topic1);
    publisher.close();
    AuditStatsQuery query=new AuditStatsQuery();
    query.fromTime = startTime;
    query.toTime = endTime;
    query.timeout = 10;
    query.filter = new Filter("topic=" + topic1);
    query.groupBy = new GroupBy(null);
    MessageConsumer consumer = AuditStatsQuery.getConsumer(startTime, "mock");
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    query.aggregateStats(consumer);
    Collection<Long> sent = query.getSent().values();
    assert (sent.iterator().hasNext());
    Long sentPublisher = sent.iterator().next();
    System.out.println("DATA SENT IN AUDIT QUERY " + sentPublisher);
    assert (sentPublisher == totalData / 2);

  }
  

  @Test
  public void testAuditQueryGroupFilter() throws IOException,
      InterruptedException,
      TException {
    assert (publisher instanceof com.inmobi.messaging.publisher.MockInMemoryPublisher);
    generateData(topic, topic1);
    publisher.close();
    AuditStatsQuery query = new AuditStatsQuery();
    query.fromTime = startTime;
    query.toTime = endTime;
    query.timeout = 10;
    query.filter = new Filter("topic=" + topic);
    query.groupBy = new GroupBy("tier");
    MessageConsumer consumer = AuditStatsQuery.getConsumer(startTime, "mock");
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    query.aggregateStats(consumer);

    Map<Columns, String> map = new HashMap<Columns, String>();
    map.put(Columns.TIER, "publisher");
    Group grp = query.groupBy.getGroup(map);
    assert (query.getSent().containsKey(grp));
    System.out.println("DATA SENT IN filter " + query.getSent().get(grp));
    assert (query.getSent().get(grp) == totalData / 2);
  }

  @Test
  public void testAuditQueryWhereEndTimeIsLessThanFromTime()
      throws IOException, InterruptedException, TException {
    assert (publisher instanceof com.inmobi.messaging.publisher.MockInMemoryPublisher);
    generateData(topic, topic1);
    publisher.close();
    AuditStatsQuery query = new AuditStatsQuery();
    query.fromTime = startTime;
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(startTime);
    calendar.add(Calendar.MINUTE, -1);
    query.toTime = calendar.getTime();
    query.timeout = 10;
    query.filter = new Filter("topic=" + topic1);
    query.groupBy = new GroupBy(null);
    MessageConsumer consumer = AuditStatsQuery.getConsumer(startTime, "mock");
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    query.aggregateStats(consumer);
    Collection<Long> sent = query.getSent().values();
    assert (sent.iterator().next() == 0);

  }

  @Test
  public void testAuditQueryCuttoffTime0() throws IOException,
      InterruptedException, TException {
    assert (publisher instanceof com.inmobi.messaging.publisher.MockInMemoryPublisher);
    generateData(topic, topic1);
    publisher.close();
    AuditStatsQuery query = new AuditStatsQuery();
    query.fromTime = startTime;
    query.toTime = endTime;
    query.timeout = 10;
    query.cutoffTime = 0;
    query.filter = new Filter("topic=" + topic1);
    query.groupBy = new GroupBy(null);
    MessageConsumer consumer = AuditStatsQuery.getConsumer(startTime, "mock");
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    query.aggregateStats(consumer);
    Collection<Long> sent = query.getSent().values();
    assert (!sent.iterator().hasNext());

  }

  @Test
  public void testAuditQueryValidCuttoffTime() throws IOException,
      InterruptedException, TException {
    System.out.println("STARTING");
    ClientConfig config = ClientConfig
        .load("src/test/resources/mock-publisher.properties");
    publisher = MessagePublisherFactory.create(config);
    assert (publisher instanceof com.inmobi.messaging.publisher.MockInMemoryPublisher);
    // ((MockInMemoryPublisher) (publisher)).reset();
    generateData(topic2, topic3);

    AuditStatsQuery query = new AuditStatsQuery();
    query.fromTime = startTime;
    query.toTime = endTime;
    query.timeout = 200;
    query.cutoffTime = 7000;
    System.out.println("FROM TIME " + query.fromTime.getTime() + " to time "
        + query.toTime.getTime());
    Thread.sleep(15000);
    generateData(topic2, topic3);

    publisher.close();
    System.out.println("FROM TIME " + query.fromTime.getTime() + " to time "
        + query.toTime.getTime());
    query.filter = new Filter("topic=" + topic2);
    query.groupBy = new GroupBy(null);
    MessageConsumer consumer = AuditStatsQuery.getConsumer(startTime, "mock");
    ((MockInMemoryConsumer) consumer)
        .setSource(((MockInMemoryPublisher) (publisher)).source);
    query.aggregateStats(consumer);
    Collection<Long> sent = query.getSent().values();
    assert (sent.iterator().hasNext());
    Long sentPublisher = sent.iterator().next();
    System.out.println("DATA SENT IN Valid cutoff " + sentPublisher);
    assert (sentPublisher == totalData / 2);

  }



}
