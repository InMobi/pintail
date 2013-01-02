package com.inmobi.audit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inmobi.audit.thrift.AuditPacket;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.AbstractMessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

public class AuditMessageWorker implements Runnable {

  private String                   hostname;
  private String                   tier;
  private int                      windowSizeInMins;
  private static final String      AUDIT_STREAM_TOPIC_NAME = "audit";
  private AbstractMessagePublisher publisher;
  private static final Logger      LOG                     = LoggerFactory
                                                               .getLogger(AuditMessageWorker.class);
  private final TSerializer        serializer              = new TSerializer();

  public AuditMessageWorker(String hostname, String tier, int windowSizeInMins) {
    this.hostname = hostname;
    this.tier = tier;
    this.windowSizeInMins = windowSizeInMins;
  }

  @Override
  public void run() {
    try {
    LOG.info("Running the AuditMessageWorker");
    for (Entry<String, AuditCounterAccumulator> entry : AuditMessagePublisher.topicAccumulatorMap
        .entrySet()) {
      String topic = entry.getKey();
      AuditCounterAccumulator accumulator = entry.getValue();
      Map<Long, AtomicLong> received = accumulator.getReceived();
      Map<Long, AtomicLong> sent = accumulator.getSent();
      accumulator.reset(); // resetting before creating packet to make sure that
                           // during creation of packet no more writes should
                           // occur to previous counters NOTE:there is a chance
                           // that after retrieving counters and before reset
                           // few updates may happen which could get lost
      AuditPacket packet = createPacket(topic, received, sent);
      publishPacket(packet);

    }
    } catch (Exception e) {// catching general exception so that thread should
                           // not get aborted
      LOG.error("Error in the run method", e);
    }

  }

  private void publishPacket(AuditPacket packet) {
    if (publisher == null)
      try {
        publisher = (AbstractMessagePublisher) MessagePublisherFactory.create();
        publisher.publish(AUDIT_STREAM_TOPIC_NAME,
            new Message(ByteBuffer.wrap(serializer.serialize(packet))));

      } catch (IOException e) {
        LOG.error("Error while publishing audit packet for " + packet, e);
      } catch (TException e) {
        LOG.error("Error while publishing audit packet for " + packet, e);
      }
  }


  private AuditPacket createPacket(String topic,
      Map<Long, AtomicLong> received, Map<Long, AtomicLong> sent) {
    Map<Long, Long> finalReceived = new HashMap<Long, Long>();
    Map<Long, Long> finalSent = new HashMap<Long, Long>();

    // TODO find a better way of converting Map<Long,AtomicLong> to
    // Map<Long,Long>;if any
    for (Entry<Long, AtomicLong> entry : received.entrySet()) {
      finalReceived.put(entry.getKey(), entry.getValue().get());
    }

    for (Entry<Long, AtomicLong> entry : sent.entrySet()) {
      finalSent.put(entry.getKey(), entry.getValue().get());
    }

    AuditPacket packet = new AuditPacket(System.currentTimeMillis(), topic,
        tier, hostname, windowSizeInMins, finalReceived, finalSent);
    return packet;
  }

}
