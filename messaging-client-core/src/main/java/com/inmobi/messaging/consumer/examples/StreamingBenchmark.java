package com.inmobi.messaging.consumer.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.util.ConsumerUtil;

public class StreamingBenchmark {

  static final String DELIMITER = "/t";
  static final SimpleDateFormat LogDateFormat = new SimpleDateFormat(
      "yyyy:MM:dd hh:mm:ss");

  static final int WRONG_USAGE_CODE = -1;
  static final int FAILED_CODE = 1;
  public Date consumerStartTime;
  public Date consumerEndTime;

  static int printUsage() {
    System.out.println("Usage: StreamingBenchmark  "
        + " [-producer <topic-name> <no-of-msgs> <no-of-msgs-per-sec>"
        + " [<timeoutSeconds> <msg-size> <no-of-threads>]]"
        + " [-consumer <no-of-producers> <no-of-msgs>"
        + " [<timeoutSeconds> <msg-size> <hadoopconsumerflag> ]]");
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
          if (args.length > consumerOptionIndex + 7) {
            timezone = args[consumerOptionIndex + 7];
          }
          runConsumer = true;
        }
      }
    } else {
      return printUsage();
    }

    assert (runProducer || runConsumer == true);
    StreamingBenchmarkProducer producer = null;
    StreamingBenchmarkConsumer consumer = null;
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
          numProducers,
          hadoopConsumer, msgSize);
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

    }
    statusPrinter.join();
    return exitcode;
  }

  static StreamingBenchmarkProducer createProducer(String topic, long maxSent,
      float numMsgsPerSec, int msgSize, int numThreads) throws IOException {
    return new StreamingBenchmarkProducer(topic, maxSent, numMsgsPerSec,
        msgSize, numThreads);
  }

  static StreamingBenchmarkConsumer createConsumer(ClientConfig config,
      long maxSent,
      Date startTime, int numProducers, boolean hadoopConsumer, int maxSize)
      throws IOException {
    return new StreamingBenchmarkConsumer(config, maxSent, startTime,
        numProducers,
        hadoopConsumer, maxSize);
  }



  static byte[] getMessageBytes(int msgSize) {
    byte[] msg = new byte[msgSize];
    for (int i = 0; i < msgSize; i++) {
      msg[i] = 'A';
    }
    return msg;
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



  static class StatusLogger extends Thread {
    volatile boolean stopped;
    StreamingBenchmarkProducer producer;
    StreamingBenchmarkConsumer consumer;

    StatusLogger(StreamingBenchmarkProducer producer,
        StreamingBenchmarkConsumer consumer) {
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
      if (stats == null)
        return;

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
