package com.inmobi.audit;

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.inmobi.messaging.Message;

public class TestAuditMessagePublisher {
  private static final byte[] magicbytes = { (byte) 0xAB, (byte) 0xCD,
      (byte) 0xEF };
  @Test
  public void testAttachHeaders() {
    Message m = new Message("test data".getBytes());
    Long timestamp = System.currentTimeMillis();
    Message msgWithHeaders = AuditMessagePublisher.attachHeaders(m, timestamp);
    ByteBuffer buffer = msgWithHeaders.getData();
    buffer.rewind();
    int version = buffer.get();
    assert (version == 1);
    byte[] bytes = new byte[3];
    buffer.get(bytes);
    assert (bytes[0] == magicbytes[0]);
    assert (bytes[1] == magicbytes[1]);
    assert (bytes[2] == magicbytes[2]);
    assert (buffer.getLong() == timestamp);
    int msgSize = buffer.getInt();
    byte[] msg = new byte[msgSize];
    buffer.get(msg);
    assert (new String(msg).equals("test data"));

  }

}
