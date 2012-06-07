package com.inmobi.messaging.consumer.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;
import com.inmobi.messaging.util.ConsumerUtil;

public class StreamingBenchmark {

  static final String DELIMITER = "/t";
  static final SimpleDateFormat LogDateFormat = new SimpleDateFormat(
      "yyyy:MM:dd hh:mm:ss");

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println(
          "Usage: StreamingBenchmark <no-of-msgs> <sleepMillis-every-msg>" +
          " [<timezone>]");
      System.exit(-1);
    }
    long maxSent = Long.parseLong(args[0]);
    int sleepMillis = Integer.parseInt(args[1]);
    String timezone = null;
    if (args.length == 3) {
      timezone = args[2];
    }

    ClientConfig config = ClientConfig.loadFromClasspath(
        MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
    String topic = config.getString(MessageConsumerFactory.TOPIC_NAME_KEY);
    System.out.println("Using topic: " + topic);

    Producer producer = new Producer(topic, maxSent, sleepMillis);
    Date now;
    if (timezone != null) {
      now = ConsumerUtil.getCurrenDateForTimeZone(timezone);
    } else {
      now = Calendar.getInstance().getTime(); 
    }
    System.out.println("Starting from " + now);
    
    producer.start();
    Consumer consumer = new Consumer(config, maxSent, now);
    consumer.start();

    StatusLogger statusPrinter = new StatusLogger(producer, consumer);
    statusPrinter.start();

    consumer.join();
    statusPrinter.stopped = true;
    statusPrinter.join();
    if (consumer.received != consumer.seqSet.size()) {
      System.out.println("Data validation FAILED!");
    } else {
      System.out.println("Data validation SUCCESS!");
    }
    System.exit(0);
  }

  static class Producer extends Thread {
    volatile AbstractMessagePublisher publisher;
    String topic;
    long maxSent;
    int sleepMillis;

    Producer(String topic, long maxSent, int sleepMillis) throws IOException {
      this.topic = topic;
      this.maxSent = maxSent;
      this.sleepMillis = sleepMillis;
      publisher = (AbstractMessagePublisher) MessagePublisherFactory.create();
    }

    @Override
    public void run() {
      System.out.println("Producer started!");
      for (long i = 1; i <= maxSent; i++) {
        long time = System.currentTimeMillis();
        String s = i + DELIMITER + Long.toString(time);
        Message msg = new Message(ByteBuffer.wrap(s.getBytes()));
        publisher.publish(topic, msg);

        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
      }
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        return;
      }
    }

  }

  static class Consumer extends Thread {
    volatile Set<Long> seqSet = new HashSet<Long>();
    final MessageConsumer consumer;
    final long maxSent;
    volatile long received = 0;
    volatile long totalLatency = 0;

    Consumer(ClientConfig config, long maxSent, Date startTime) 
        throws IOException {
      this.maxSent = maxSent;
      consumer = MessageConsumerFactory.create(config, startTime);
    }

    @Override
    public void run() {
      System.out.println("Consumer started!");
      while (true) {
        Message msg = null;
        try {
          msg = consumer.next();
          received++;
          String s = new String(msg.getData().array());
          String[] ar = s.split(DELIMITER);
          long seq = Long.parseLong(ar[0]);
          seqSet.add(seq);
          long sentTime = Long.parseLong(ar[1]);
          totalLatency += System.currentTimeMillis() - sentTime;
          if (received == maxSent) {
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
      }
      consumer.close();
    }

  }

  static class StatusLogger extends Thread {
    volatile boolean stopped;
    Producer producer;
    Consumer consumer;
    StatusLogger(Producer producer, Consumer consumer) {
      this.producer = producer;
      this.consumer = consumer;
    }
    @Override
    public void run() {
      while(!stopped) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
        StringBuffer sb = new StringBuffer();
        sb.append(LogDateFormat.format(System.currentTimeMillis()));
        sb.append(" Invocations:" + producer.publisher.getStats().
            getInvocationCount());
        sb.append(" SentSuccess:" + producer.publisher.getStats().
            getSuccessCount());
        sb.append(" UnhandledExceptions:" + producer.publisher.getStats().
            getUnhandledExceptionCount());
        sb.append(" Received:" + consumer.received);
        sb.append(" UniqueReceived:" + consumer.seqSet.size());
        if (consumer.received != 0) {
          sb.append(" MeanLatency(ms):" 
              + (consumer.totalLatency / consumer.received));
        }
        System.out.println(sb.toString());
      }
    }
  }
}
