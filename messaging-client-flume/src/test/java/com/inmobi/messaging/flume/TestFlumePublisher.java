package com.inmobi.messaging.flume;

import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.flume.api.RpcClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.Message;

public class TestFlumePublisher {

  private RpcClient mockRpcClient;
  private FlumeMessagePublisher publisher;
  private String topic = "test";

  @BeforeMethod
  public void init() throws IOException {
    mockRpcClient = Mockito.mock(RpcClient.class);
    ClientConfig config = new ClientConfig();
    publisher = new MockFlumePublisher();
    publisher.init(config);
  }

  @Test
  public void testSuccess() throws Exception {
    Message msg = new Message("msg".getBytes());
    publisher.publish(topic, msg);

    // Wait for all operations to complete
    waitToComplete();
    verify(mockRpcClient, times(1)).appendBatch(Mockito.anyList());
    Assert.assertEquals(publisher.getStats(topic).getInvocationCount(), 1,
        "invocation count");
    Assert.assertEquals(publisher.getStats(topic).getSuccessCount(), 1,
        "success count");
    Assert.assertEquals(publisher.getStats(topic).getUnhandledExceptionCount(),
        0, "unhandled exception count");

    publisher.close();
    verify(mockRpcClient, times(1)).close();
  }

  @Test
  public void testFailure() throws Exception {
    doThrow(new RuntimeException()).when(mockRpcClient).appendBatch(anyList());

    Message msg = new Message("msg".getBytes());
    publisher.publish("test", msg);

    // Wait for all operations to complete
    waitToComplete();
    verify(mockRpcClient, times(1)).appendBatch(Mockito.anyList());
    Assert.assertEquals(publisher.getStats(topic).getInvocationCount(), 1,
        "invocation count");
    Assert.assertEquals(publisher.getStats(topic).getSuccessCount(), 0,
        "success count");
    Assert.assertEquals(publisher.getStats(topic).getUnhandledExceptionCount(), 1,
        "unhandled exception count");

    publisher.close();
    verify(mockRpcClient, times(1)).close();
  }

  private void waitToComplete() throws InterruptedException {
    int i = 0;
    while (publisher.getStats(topic).getInFlight() != 0 && i++ < 10) {
      Thread.sleep(100);
    }
  }

  class MockFlumePublisher extends FlumeMessagePublisher {

    @Override
    protected RpcClient createRpcClient(ClientConfig config) {
      return mockRpcClient;
    }
  }
}
