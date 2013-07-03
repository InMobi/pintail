package com.inmobi.messaging.consumer.examples;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;

public class StreamingBenchmarkConsumer extends Thread {

  final TreeMap<Long, Integer> messageToProducerCount;
  final MessageConsumer consumer;
  final long maxSent;
  volatile long received = 0;
  volatile long totalLatency = 0;
  int numProducers;
  public boolean success = false;
  boolean hadoopConsumer = false;
  int numDuplicates = 0;
  long nextElementToPurge = 1;
  String fixedMsg;
  int mismatches = 0;
  int corrupt = 0;

  public StreamingBenchmarkConsumer(ClientConfig config, long maxSent,
      Date startTime,
      int numProducers, boolean hadoopConsumer, int msgSize) throws IOException {
    this.maxSent = maxSent;
    messageToProducerCount = new TreeMap<Long, Integer>();
    this.numProducers = numProducers;
    consumer = MessageConsumerFactory.create(config, startTime);
    this.hadoopConsumer = hadoopConsumer;
    this.fixedMsg = new String(StreamingBenchmark.getMessageBytes(msgSize));
  }

  private void purgeCounts() {
    Set<Map.Entry<Long, Integer>> entrySet = messageToProducerCount.entrySet();
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
        String s = StreamingBenchmark.getMessage(msg, hadoopConsumer);
        String[] ar = s.split(StreamingBenchmark.DELIMITER);
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
      Set<Map.Entry<Long, Integer>> entrySet = messageToProducerCount
          .entrySet();
      if (entrySet.size() != 1) {
        // could happen in the case where messages are received by the
        // consumer after the purging has been done for that message's index
        // i.e older messages
        System.out.println("More than one entries in the message-producer map");
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
