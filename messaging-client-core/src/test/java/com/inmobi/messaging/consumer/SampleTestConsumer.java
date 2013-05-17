package com.inmobi.messaging.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class SampleTestConsumer extends MockConsumer {

  public BlockingQueue<byte[]> messages = new LinkedBlockingQueue<byte[]>();

  private List<byte[]> createData() throws IOException, TException {

    Map<Long, Long> received = new HashMap<Long, Long>();
    long time = System.currentTimeMillis() - 60000;
    long window = time - time % 60000;
    received.put(window, 100l);
    AuditMessage packet = new AuditMessage(System.currentTimeMillis(),
        "testTopic", "publisher", "localhost", 1, received, received, null,
        null);
    TSerializer serializer = new TSerializer();
    // serializer.serialize(packet);
    byte[] output = serializer.serialize(packet);
    List<byte[]> result = new ArrayList<byte[]>();
    result.add(output);
    return result;
  }

  public void init(String topicName, String consumerName, Date startTimestamp,
      ClientConfig config) throws IOException {
    super.init(topicName, consumerName, startTimestamp, config);
    try {
      setMessages(createData());
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void setMessages(List<byte[]> messages) {
    this.messages.addAll(messages);
  }



  @Override
  protected Message getNext() throws InterruptedException, EndOfStreamException {
    return new Message(messages.take());
    
  }

}
