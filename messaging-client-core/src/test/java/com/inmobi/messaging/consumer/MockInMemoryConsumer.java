package com.inmobi.messaging.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;
import com.inmobi.messaging.Message;

public class MockInMemoryConsumer extends AbstractMessageConsumer {

  private Map<String, BlockingQueue<Message>> source;
  private int offset = 0;
  private static final byte[] magicBytes = { (byte) 0xAB, (byte) 0xCD,
      (byte) 0xEF };
  private static final byte[] versions = { 1 };
  private static final int HEADER_LENGTH = 16;

  public void setSource(Map<String, BlockingQueue<Message>> source) {
    this.source = source;
  }

  @Override
  public boolean isMarkSupported() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  protected AbstractMessagingClientStatsExposer getMetricsImpl() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void doMark() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  protected void doReset() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  protected Message getNext() throws InterruptedException {
    BlockingQueue<Message> queue = source.get(topicName);
    if (queue == null)
      queue = new LinkedBlockingQueue<Message>();
    Message msg = queue.take();
    msg.set(removeHeader(msg.getData().array()));
    return msg;
  }

  public synchronized Message next() throws InterruptedException {
    Message msg = getNext();
    return msg;
  }

  public static ByteBuffer removeHeader(byte data[]) {
    boolean isValidHeaders = true;
    if (data.length < 16) {
      isValidHeaders = false;
    }
    ByteBuffer buffer = ByteBuffer.wrap(data);
    boolean isVersionValid = false;
    if (isValidHeaders) {
      for (byte version : versions) {
        if (buffer.get() == version) {
          isVersionValid = true;
          break;
        }
      }
      if (isVersionValid) {
        // compare all 3 magicBytes
        byte[] mBytesRead = new byte[3];
        buffer.get(mBytesRead);
        if (mBytesRead[0] != magicBytes[0] || mBytesRead[1] != magicBytes[1]
            || mBytesRead[2] != magicBytes[2])
          isValidHeaders = false;
      }

      if (isValidHeaders) {
        // TODO add validation for timestamp
        long timestamp = buffer.getLong();

        int messageSize = buffer.getInt();
        if (isValidHeaders && data.length != HEADER_LENGTH + messageSize) {
          isValidHeaders = false;
        }
      }
    }


    if (isValidHeaders) {
      return ByteBuffer.wrap(Arrays.copyOfRange(data, HEADER_LENGTH,
          data.length));
    } else
      return ByteBuffer.wrap(data);
  }


}
