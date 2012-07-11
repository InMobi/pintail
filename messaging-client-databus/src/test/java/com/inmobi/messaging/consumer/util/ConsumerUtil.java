package com.inmobi.messaging.consumer.util;

import java.io.IOException;
import java.util.Date;

import org.testng.Assert;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.consumer.databus.AbstractMessagingDatabusConsumer;
import com.inmobi.messaging.consumer.databus.Checkpoint;
import com.inmobi.messaging.consumer.databus.DatabusConsumer;
import com.inmobi.messaging.consumer.hadoop.HadoopConsumer;

public class ConsumerUtil {

  public static void assertMessages(ClientConfig config, String streamName,
      String consumerName, int numClusters, int numCollectors, int numDataFiles,
      int numMessagesPerFile, boolean hadoop)
          throws IOException, InterruptedException {
    int numCounters = numClusters * numCollectors;
    int totalMessages = numCounters * numDataFiles * numMessagesPerFile;
    int[] counter = new int[numCounters];
    for (int i = 0; i <numCounters; i++) {
      counter[i] = 0;
    }
    int[] markedcounter = new int[numCounters];
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);

    consumer.init(streamName, consumerName, null, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);

    for (int i = 0; i < totalMessages/2; i++) {
      Message msg = consumer.next();
      String msgStr = getMessage(msg.getData().array(), hadoop);
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(counter[m]))) {
          counter[m]++;
          break;
        }
      }
    }
    consumer.mark();
    for (int i = 0; i < numCounters; i++) {
      markedcounter[i] = counter[i];
    }

    for (int i = 0; i < totalMessages/2; i++) {
      Message msg = consumer.next();
      String msgStr = getMessage(msg.getData().array(), hadoop);
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(counter[m]))) {
          counter[m]++;
          break;
        }
      }
    }    
    for (int i= 0; i < numCounters; i++) {
      Assert.assertEquals(counter[i], numDataFiles * numMessagesPerFile);
    }

    consumer.reset();

    for (int i = 0; i < totalMessages/2; i++) {
      Message msg = consumer.next();
      String msgStr = getMessage(msg.getData().array(), hadoop);
      for (int m = 0;  m < numCounters; m++) {
        if (msgStr.equals(MessageUtil.constructMessage(markedcounter[m]))) {
          markedcounter[m]++;
          break;
        }
      }
    }

    for (int i= 0; i < numCounters; i++) {
      Assert.assertEquals(markedcounter[i], numDataFiles * numMessagesPerFile);
    }
    consumer.close();
  }

  private static AbstractMessagingDatabusConsumer createConsumer(boolean hadoop) {
    if (hadoop) {
      return new HadoopConsumer();
    } else {
      return new DatabusConsumer();
    }
  }

  private static String getMessage(byte[] array, boolean hadoop) throws IOException {
    if (hadoop) {
      return MessageUtil.getTextMessage(array).toString();
    } else {
      return new String(array);
    }
  }

  public static void testMarkAndResetWithStartTime(ClientConfig config,
      String streamName, String consumerName, Date startTime, boolean hadoop)
          throws Exception {
    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);

    consumer.init(streamName, consumerName,
        startTime, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);

    int i;
    for (i = 100; i < 120; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark(); 
    for (i = 120; i < 130; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();

    for (i = 120; i < 240; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.mark();
    Checkpoint lastCheckpoint = new Checkpoint(
        consumer.getCurrentCheckpoint().toBytes());

    for (i = 240; i < 260; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();
    for (i = 240; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.close();

    // test checkpoint and consumer crash
    consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    Assert.assertEquals(consumer.getCurrentCheckpoint(), lastCheckpoint);

    for (i = 240; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();

    consumer.close();
  }

  public static void testMarkAndReset(ClientConfig config, String streamName,
      String consumerName, boolean hadoop) throws Exception {

    AbstractMessagingDatabusConsumer consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    Assert.assertEquals(consumer.getTopicName(), streamName);
    Assert.assertEquals(consumer.getConsumerName(), consumerName);
    Assert.assertEquals(consumer.getPartitionReaders().size(), 1);

    int i;
    for (i = 0; i < 20; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark(); 
    for (i = 20; i < 30; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();

    for (i = 20; i < 140; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.mark();
    Checkpoint lastCheckpoint = new Checkpoint(
        consumer.getCurrentCheckpoint().toBytes());

    for (i = 140; i < 160; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.reset();
    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }

    consumer.close();

    // test checkpoint and consumer crash
    consumer = createConsumer(hadoop);
    consumer.init(streamName, consumerName, null, config);
    Assert.assertEquals(consumer.getCurrentCheckpoint(), lastCheckpoint);

    for (i = 140; i < 300; i++) {
      Message msg = consumer.next();
      Assert.assertEquals(getMessage(msg.getData().array(), hadoop),
          MessageUtil.constructMessage(i));
    }
    consumer.mark();

    consumer.close();

  }

}
