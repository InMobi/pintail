package com.inmobi.messaging.consumer.examples;

import com.inmobi.instrumentation.TimingAccumulator;

public class StreamingBenchmarkStatusLogger extends Thread {

  public volatile boolean stopped;
  StreamingBenchmarkProducer producer;
  StreamingBenchmarkConsumer consumer;

  public StreamingBenchmarkStatusLogger(StreamingBenchmarkProducer producer,
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
      sb.append(StreamingBenchmark.LogDateFormat.format(System
          .currentTimeMillis()));
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
