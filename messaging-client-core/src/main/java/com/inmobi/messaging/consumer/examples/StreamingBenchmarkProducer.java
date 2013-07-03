package com.inmobi.messaging.consumer.examples;

import java.io.IOException;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

public class StreamingBenchmarkProducer extends Thread {

  volatile AbstractMessagePublisher publisher;
  final String topic;
  final long maxSent;
  final long sleepMillis;
  final long numMsgsPerSleepInterval;
  final int numThreads;
  public int exitcode = StreamingBenchmark.FAILED_CODE;
  byte[] fixedMsg;
  ProducerWoker[] workerThreads = null;

  public StreamingBenchmarkProducer(String topic, long maxSent,
      float numMsgsPerSec,
      int msgSize, int numThreads) throws IOException {
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
    fixedMsg = StreamingBenchmark.getMessageBytes(msgSize);
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
    System.out.println(StreamingBenchmark.LogDateFormat.format(System
        .currentTimeMillis())
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
    System.out.println(StreamingBenchmark.LogDateFormat.format(System
        .currentTimeMillis())
        + " Producer closed");
    TimingAccumulator stats = publisher.getStats(topic);
    if (stats != null && stats.getSuccessCount() == maxSent * numThreads) {
      exitcode = 0;
    }
  }

  private class ProducerWoker extends Thread {
    @Override
    public void run() {
      System.out.println(StreamingBenchmark.LogDateFormat.format(System
          .currentTimeMillis())
          + " Producer worker started!");
      long msgIndex = 1;
      boolean sentAll = false;
      long startTime = 0l;
      long endTime = 0l;
      long publishTime = 0l;

      while (true) {
        for (long j = 0; j < numMsgsPerSleepInterval; j++) {
          Message m = StreamingBenchmark.constructMessage(msgIndex, fixedMsg);

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

      System.out.println(StreamingBenchmark.LogDateFormat.format(System
          .currentTimeMillis())
          + " Producer worker closed." + " Messages published: "
          + Long.toString(msgIndex) + " Total publish time (ms): "
          + Long.toString(publishTime));
    } // run()
  } // ProducerWorker

}
