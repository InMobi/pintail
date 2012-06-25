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

  static void printUsage() {
    System.out.println(
        "Usage: StreamingBenchmark  " +
        " [-producer <topic-name> <no-of-msgs> <sleepMillis-every-msg> ]" +
        " [-consumer <no-of-producers> <no-of-msgs> [<timezone>] ]");
    System.exit(-1);    
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      printUsage();
    }
    long maxSent = -1;
    int sleepMillis = -1;
    String timezone = null;
    String topic = null;
    int numProducers = 1;
    boolean runProducer = false;
    boolean runConsumer = false;

    if (args.length >= 3) {
      int consumerOptionIndex = -1;
      if (args[0].equals("-producer")) {
        topic = args[1];
        maxSent = Long.parseLong(args[2]);
        sleepMillis = Integer.parseInt(args[3]);
        runProducer = true;
        consumerOptionIndex = 4;
      } else {
        consumerOptionIndex = 0;
      }
      
      if (args.length > consumerOptionIndex) {
        if (args[consumerOptionIndex].equals("-consumer")) {
          numProducers = Integer.parseInt(args[consumerOptionIndex + 1]);
          maxSent = Long.parseLong(args[consumerOptionIndex + 2]);
          if (args.length > consumerOptionIndex + 3) {
            timezone = args[consumerOptionIndex + 3];
          }
          runConsumer = true;
        }
      }
    } else {
      printUsage();
    }

    assert(runProducer || runConsumer == true);
    Producer producer = null;
    Consumer consumer = null;
    StatusLogger statusPrinter;

    if (runProducer) {
     System.out.println("Using topic: " + topic);
      producer = createProducer(topic, maxSent, sleepMillis);
      producer.start();
    }
    
    if (runConsumer) {
      ClientConfig config = ClientConfig.loadFromClasspath(
          MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
      Date now;
      if (timezone != null) {
        now = ConsumerUtil.getCurrenDateForTimeZone(timezone);
      } else {
        now = Calendar.getInstance().getTime(); 
      }
      System.out.println("Starting from " + now);

      // create and start consumer
      assert(config != null);
      consumer = createConsumer(config, maxSent, now, numProducers);
      consumer.start();
    }
    
    statusPrinter = new StatusLogger(producer, consumer);
    statusPrinter.start();

    
    if (runProducer) {
      assert (producer != null);
      producer.join();
      if (!runConsumer) {
        statusPrinter.stopped = true;
      }
    } 
    if (runConsumer) {
      assert (consumer !=null);
      consumer.join();
      statusPrinter.stopped = true;
    }

    statusPrinter.join();
    if (runConsumer) {
      if (!consumer.success) {
        System.out.println("Data validation FAILED!");
      } else {
        System.out.println("Data validation SUCCESS!");
      }
    }
    System.exit(0);
  }

  static Producer createProducer(String topic, long maxSent, int sleepMillis)
      throws IOException {
    return new Producer(topic, maxSent, sleepMillis); 
  }

  static Consumer createConsumer(ClientConfig config, long maxSent,
      Date startTime, int numProducers) throws IOException {
    return new Consumer(config, maxSent, startTime, numProducers);    
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
      publisher.close();
    }

  }

  static class Consumer extends Thread {
    //volatile Set<Long> seqSet = new HashSet<Long>();
    final MessageConsumer consumer;
    final long maxSent;
    volatile long received = 0;
    volatile long totalLatency = 0;
    long[] counter;
    int numProducers;
    boolean success = false;

    Consumer(ClientConfig config, long maxSent, Date startTime,
        int numProducers) throws IOException {
      this.maxSent = maxSent;
      consumer = MessageConsumerFactory.create(config, startTime);
      this.numProducers = numProducers;
      counter = new long[numProducers];
      for (int i = 0; i < numProducers; i++) {
        counter[i] = 0;
      }
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
          int m;
          for (m = 0;  m < numProducers; m++) {
            if (seq == (counter[m] + 1)) {
              counter[m]++;
              break;
            }
          }
          if (m == numProducers) {
            throw new RuntimeException("Data outof order!");
          }
          long sentTime = Long.parseLong(ar[1]);
          totalLatency += System.currentTimeMillis() - sentTime;
          if (received == maxSent * numProducers) {
            break;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        } catch (Exception e) {
          System.out.println("Got exception for " + 
            new String(msg.getData().array()));
          e.printStackTrace();
        }
      }
      for (int i = 0; i < numProducers; i++) {
        if (counter[i] != maxSent) {
          success = false;
          break;
        } else {
          success = true;
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
        if (producer != null) {
          constructProducerString(sb);
        }
        if (consumer != null) {
          constructConsumerString(sb);
        }
        System.out.println(sb.toString());
      }
    }
    
    void constructProducerString(StringBuffer sb) {
      sb.append(" Invocations:" + producer.publisher.getStats().
          getInvocationCount());
      sb.append(" SentSuccess:" + producer.publisher.getStats().
          getSuccessCount());
      sb.append(" UnhandledExceptions:" + producer.publisher.getStats().
          getUnhandledExceptionCount());      
    }
    
    void constructConsumerString(StringBuffer sb) {
      sb.append(" Received:" + consumer.received);
      sb.append(" UniqueReceived:");
      for (int i = 0; i< consumer.numProducers; i++) {
        sb.append(consumer.counter[i]);
        sb.append(",");
      }
      if (consumer.received != 0) {
        sb.append(" MeanLatency(ms):" 
            + (consumer.totalLatency / consumer.received));
      }      
    }
  }
}
