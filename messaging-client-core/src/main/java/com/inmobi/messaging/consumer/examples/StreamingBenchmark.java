package com.inmobi.messaging.consumer.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.inmobi.instrumentation.TimingAccumulator;
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

  static final int WRONG_USAGE_CODE = -1;
  static final int FAILED_CODE = 1;
  private Date consumerStartTime;
  private Date consumerEndTime;

  static int printUsage() {
    System.out.println("Usage: StreamingBenchmark  "
        + " [-producer <topic-name> <no-of-msgs> <no-of-msgs-per-sec>"
        + " [<timeoutSeconds> <msg-size> <no-of-threads>]]"
        + " [-consumer <no-of-producers> <no-of-msgs>"
        + " [<timeoutSeconds> <msg-size> <hadoopconsumerflag><timezone>]]");
    return WRONG_USAGE_CODE;
  }

  public static void main(String[] args) throws Exception {
    StreamingBenchmark benchmark = new StreamingBenchmark();
    int exitcode = benchmark.run(args);
    System.exit(exitcode);
  }

  static int numProducerArgs = 5;
  static int numProducerRequiredArgs = 3;
  static int numConsumerArgs = 5;
  static int numConsumerRequiredArgs = 2;
  static int minArgs = 3;

  public int run(String[] args) throws Exception {
    if (args.length < minArgs) {
      return printUsage();
    }
    long maxSent = -1;
    float numMsgsPerSec = -1;
    String timezone = null;
    String topic = null;
    int numProducers = 1;
    boolean runProducer = false;
    boolean runConsumer = false;
    boolean hadoopConsumer = false;
    int producerTimeout = 0;
    int numProducerThreads = 1;
    int consumerTimeout = 0;
    int msgSize = 2000;

    if (args.length >= minArgs) {
      int consumerOptionIndex = -1;
      if (args[0].equals("-producer")) {
        if (args.length < (numProducerRequiredArgs + 1)) {
          return printUsage();
        }
        topic = args[1];
        maxSent = Long.parseLong(args[2]);
        numMsgsPerSec = Float.parseFloat(args[3]);
        runProducer = true;
        if (args.length > 4 && !args[4].equals("-consumer")) {
          producerTimeout = Integer.parseInt(args[4]);
          System.out
              .println("producerTimeout :" + producerTimeout + " seconds");
          if (args.length > 5 && !args[5].equals("-consumer")) {
            msgSize = Integer.parseInt(args[5]);
            if (args.length > 6 && !args[6].equals("-consumer")) {
              numProducerThreads = Integer.parseInt(args[6]);
              consumerOptionIndex = 7;
            } else {
              consumerOptionIndex = 6;
            }
          } else {
            consumerOptionIndex = 5;
          }
        } else {
          consumerOptionIndex = 4;
        }
      } else {
        consumerOptionIndex = 0;
      }

      if (args.length > consumerOptionIndex) {
        if (args[consumerOptionIndex].equals("-consumer")) {
          numProducers = Integer.parseInt(args[consumerOptionIndex + 1]);
          maxSent = Long.parseLong(args[consumerOptionIndex + 2]);
          if (args.length > consumerOptionIndex + 3) {
            consumerTimeout = Integer.parseInt(args[consumerOptionIndex + 3]);
            System.out.println("consumerTimeout :" + consumerTimeout
                + " seconds");
          }
          if (args.length > consumerOptionIndex + 4) {
            msgSize = Integer.parseInt(args[consumerOptionIndex + 4]);
          }
          if (args.length > consumerOptionIndex + 5) {
            hadoopConsumer = (Integer.parseInt(args[consumerOptionIndex + 5]) > 0);
          }
          if (args.length > consumerOptionIndex + 6) {
            timezone = args[consumerOptionIndex + 6];
          }
          runConsumer = true;
        }
      }
    } else {
      return printUsage();
    }

    assert (runProducer || runConsumer);
    Producer producer = null;
    Consumer consumer = null;
    StatusLogger statusPrinter;

    if (runProducer) {
      System.out.println("Using topic: " + topic);
      producer = createProducer(topic, maxSent, numMsgsPerSec, msgSize,
          numProducerThreads);
      producer.start();
    }

    if (runConsumer) {
      ClientConfig config = ClientConfig
          .loadFromClasspath(MessageConsumerFactory.MESSAGE_CLIENT_CONF_FILE);
      if (timezone != null) {
        consumerStartTime = ConsumerUtil.getCurrenDateForTimeZone(timezone);
      } else {
        consumerStartTime = Calendar.getInstance().getTime();
      }
      System.out.println("Starting from " + consumerStartTime);

      // create and start consumer
      assert (config != null);
      consumer = createConsumer(config, maxSent, consumerStartTime,
          numProducers, hadoopConsumer, msgSize);
      consumer.start();
    }

    statusPrinter = new StatusLogger(producer, consumer);
    statusPrinter.start();

    int exitcode = 0;
    if (runProducer) {
      assert (producer != null);
      producer.join(producerTimeout * 1000);
      System.out.println("Producer thread state: " + producer.getState());
      exitcode = producer.exitcode;
      if (exitcode == FAILED_CODE) {
        System.out.println("Producer FAILED!");
      } else {
        System.out.println("Producer SUCCESS!");
      }
      if (!runConsumer) {
        statusPrinter.stopped = true;
      }
    }
    if (runConsumer) {
      assert (consumer != null);
      consumer.join(consumerTimeout * 1000);
      System.out.println("Consumer thread state: " + consumer.getState());
      statusPrinter.stopped = true;
    }
    statusPrinter.join();

    if (runConsumer) {
      // set consumer end time as auditEndTime
      if (timezone != null) {
        consumerEndTime = ConsumerUtil.getCurrenDateForTimeZone(timezone);
      } else {
        consumerEndTime = Calendar.getInstance().getTime();
      }
      if (!consumer.success) {
        System.out.println("Data validation FAILED!");
        exitcode = FAILED_CODE;
      } else {
        System.out.println("Data validation SUCCESS!");
      }
      System.out.println("Consumer end time is " + consumerEndTime);
    }
    return exitcode;
  }

  static Producer createProducer(String topic, long maxSent,
      float numMsgsPerSec, int msgSize, int numThreads) throws IOException {
    return new Producer(topic, maxSent, numMsgsPerSec, msgSize, numThreads);
  }

  static Consumer createConsumer(ClientConfig config, long maxSent,
      Date startTime, int numProducers, boolean hadoopConsumer, int maxSize)
      throws IOException {
    return new Consumer(config, maxSent, startTime, numProducers,
        hadoopConsumer, maxSize);
  }


  static class Producer extends Thread {
    volatile AbstractMessagePublisher publisher;
    final String topic;
    final long maxSent;
    final long sleepMillis;
    final long numMsgsPerSleepInterval;
    final int numThreads;
    int exitcode = FAILED_CODE;
    byte[] fixedMsg;
    ProducerWoker [] workerThreads = null;

    Producer(String topic, long maxSent, float numMsgsPerSec, int msgSize,
        int numThreads)
        throws IOException {
      this.topic = topic;
      this.maxSent = maxSent;
      if (maxSent <= 0) {
        throw new IllegalArgumentException("Invalid total number of messages");
      }
      if (numMsgsPerSec > 1000) {
        this.sleepMillis = 1;
        numMsgsPerSleepInterval = (int) (numMsgsPerSec / 1000);
      } else {
        if (numMsgsPerSec <= 0) {
          throw new IllegalArgumentException("Invalid number of messages per"
              + " second");
        }
        this.sleepMillis = (int) (1000 / numMsgsPerSec);
        numMsgsPerSleepInterval = 1;
      }
      fixedMsg = getMessageBytes(msgSize);
      this.numThreads = numThreads;
      publisher = (AbstractMessagePublisher) MessagePublisherFactory.create();

      // create producer workers
      workerThreads = new ProducerWoker[numThreads];
      for (int i = 0; i < numThreads; i++) {
        ProducerWoker worker = new ProducerWoker();
        workerThreads[i] = worker;
      }
    }

    @Override
    public void run() {
      System.out.println(LogDateFormat.format(System.currentTimeMillis())
          + " Producer started!");
      // start producer worker threads
      for (int i = 0; i < numThreads; i++) {
        workerThreads[i].start();
      }

      // wait for worker threads to join
      for (int i = 0; i < numThreads; i++) {
        try {
          workerThreads[i].join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      // it is safe to close the publisher since all workers threads have
      // finished by now.
      publisher.close();
      System.out.println(LogDateFormat.format(System.currentTimeMillis())
          + " Producer closed");
      TimingAccumulator stats = publisher.getStats(topic);
      if (stats != null && stats.getSuccessCount() == maxSent * numThreads) {
        exitcode = 0;
      }
    }

    private class ProducerWoker extends Thread {
      @Override
      public void run() {
        System.out.println(LogDateFormat.format(System.currentTimeMillis())
            + " Producer worker started!");
        long msgIndex = 1;
        boolean sentAll = false;
        long startTime = 0L;
        long endTime = 0L;
        long publishTime = 0L;

        while (true) {
          for (long j = 0; j < numMsgsPerSleepInterval; j++) {
            Message m = constructMessage(msgIndex, fixedMsg);

            startTime = System.currentTimeMillis();
            publisher.publish(topic, m);
            endTime = System.currentTimeMillis();
            publishTime += endTime - startTime;

            if (msgIndex == maxSent) {
              sentAll = true;
              break;
            }
            msgIndex++;
          }
          if (sentAll) {
            break;
          }
          try {
            Thread.sleep(sleepMillis);
          } catch (InterruptedException e) {
            e.printStackTrace();
            return;
          }
        }

        System.out.println(LogDateFormat.format(System.currentTimeMillis())
            + " Producer worker closed."
            + " Messages published: " + Long.toString(msgIndex)
            + " Total publish time (ms): " + Long.toString(publishTime));
      } // run()
    } // ProducerWorker
  }

  static byte[] getMessageBytes(int msgSize) {
    byte[] msg = new byte[msgSize];
    for (int i = 0; i < msgSize; i++) {
      msg[i] = 'A';
    }
    return msg;
  }

  public Date getConsumerStartTime() {
    return consumerStartTime;
  }

  public Date getConsumerEndTime() {
    return consumerEndTime;
  }

  static Message constructMessage(long msgIndex, byte[] randomBytes) {
    long time = System.currentTimeMillis();
    String s = msgIndex + DELIMITER + Long.toString(time) + DELIMITER;
    byte[] msgBytes = new byte[s.length() + randomBytes.length];
    System.arraycopy(s.getBytes(), 0, msgBytes, 0, s.length());
    System.arraycopy(randomBytes, 0, msgBytes, s.length(), randomBytes.length);
    return new Message(ByteBuffer.wrap(msgBytes));
  }

  static String getMessage(Message msg, boolean hadoopConsumer)
      throws IOException {
    byte[] data = msg.getData().array();
    return new String(data);
  }

  static class Consumer extends Thread {
    final TreeMap<Long, Integer> messageToProducerCount;
    final MessageConsumer consumer;
    final long maxSent;
    volatile long received = 0;
    volatile long totalLatency = 0;
    int numProducers;
    boolean success = false;
    boolean hadoopConsumer = false;
    int numDuplicates = 0;
    long nextElementToPurge = 1;
    String fixedMsg;
    int mismatches = 0;
    int corrupt = 0;

    Consumer(ClientConfig config, long maxSent, Date startTime,
        int numProducers, boolean hadoopConsumer, int msgSize)
        throws IOException {
      this.maxSent = maxSent;
      messageToProducerCount = new TreeMap<Long, Integer>();
      this.numProducers = numProducers;
      consumer = MessageConsumerFactory.create(config, startTime);
      this.hadoopConsumer = hadoopConsumer;
      this.fixedMsg = new String(getMessageBytes(msgSize));
    }

    private void purgeCounts() {
      Set<Map.Entry<Long, Integer>> entrySet =
          messageToProducerCount.entrySet();
      Iterator<Map.Entry<Long, Integer>> iter = entrySet.iterator();
      while (iter.hasNext()) {
        Map.Entry<Long, Integer> entry = iter.next();
        long msgIndex = entry.getKey();
        int pcount = entry.getValue();
        if (messageToProducerCount.size() > 1) {
          if (msgIndex == nextElementToPurge) {
            if (pcount >= numProducers) {
              iter.remove();
              nextElementToPurge++;
              if (pcount > numProducers) {
                numDuplicates += (pcount - numProducers);
              }
              continue;
            }
          }
        }
        break;
      }
    }

    @Override
    public void run() {
      System.out.println("Consumer started!");
      while (true) {
        if (received == maxSent * numProducers) {
          break;
        }
        Message msg = null;
        try {
          msg = consumer.next();
          received++;
          String s = getMessage(msg, hadoopConsumer);
          String[] ar = s.split(DELIMITER);
          Long seq = Long.parseLong(ar[0]);
          Integer pcount = messageToProducerCount.get(seq);
          if (seq < nextElementToPurge) {
            numDuplicates++;
          } else {
            if (pcount == null) {
              messageToProducerCount.put(seq, new Integer(1));
            } else {
              pcount++;
              messageToProducerCount.put(seq, pcount);
            }
            long sentTime = Long.parseLong(ar[1]);
            totalLatency += System.currentTimeMillis() - sentTime;

            if (!fixedMsg.equals(ar[2])) {
              mismatches++;
            }
          }
          purgeCounts();
        } catch (Exception e) {
          corrupt++;
          e.printStackTrace();
        }
      }
      purgeCounts();
      for (int pcount : messageToProducerCount.values()) {
        if (pcount > numProducers) {
          numDuplicates += (pcount - numProducers);
        }
      }
      if (numDuplicates != 0) {
        success = false;
      } else {
        Set<Map.Entry<Long, Integer>> entrySet =
            messageToProducerCount.entrySet();
        if (entrySet.size() != 1) {
          // could happen in the case where messages are received by the
          // consumer after the purging has been done for that message's index
          // i.e older messages
          System.out
              .println("More than one entries in the message-producer map");
          success = false;
        } else {
          // the last entry in the message-producer map should be that of the
          // last msg sent i.e. msgIndex should be maxSent as purging would not
          // happen unless the size of the map is > 1 and for the last message
          // the size of map would be 1
          for (Map.Entry<Long, Integer> entry : entrySet) {
            long msgIndex = entry.getKey();
            int pcount = entry.getValue();
            if (msgIndex == maxSent) {
              if (pcount != numProducers) {
                System.out.println("No of msgs received for the last msg != "
                    + "numProducers");
                System.out.println("Expected " + numProducers + " Received "
                    + pcount);
                success = false;
                break;
              } else {
                success = true;
              }
            } else {
              System.out
                  .println("The last entry is not that of the last msg sent");
              success = false;
              break;
            }
          }
        }
      }
      if (mismatches != 0) {
        System.out.println("Number of mismatches:" + mismatches);
        success = false;
      }
      if (corrupt != 0) {
        System.out.println("Corrupt messages:" + corrupt);
        success = false;
      }
      consumer.close();
      System.out.println("Consumer closed");
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
      while (!stopped) {
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
      // check whether TimingAccumulator is created for this topic
      TimingAccumulator stats = producer.publisher.getStats(producer.topic);
      if (stats == null) {
        return;
      }

      sb.append(" Invocations:" + stats.getInvocationCount());
      sb.append(" Inflight:" + stats.getInFlight());
      sb.append(" SentSuccess:" + stats.getSuccessCount());
      sb.append(" Lost:" + stats.getLostCount());
      sb.append(" GracefulTerminates:" + stats.getGracefulTerminates());
      sb.append(" UnhandledExceptions:" + stats.getUnhandledExceptionCount());
    }

    void constructConsumerString(StringBuffer sb) {
      sb.append(" Received:" + consumer.received);
      sb.append(" Duplicates:");
      sb.append(consumer.numDuplicates);
      if (consumer.received != 0) {
        sb.append(" MeanLatency(ms):"
            + (consumer.totalLatency / consumer.received));
      }
    }
  }

}
