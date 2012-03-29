package com.inmobi.messaging.netty;

import static org.testng.Assert.assertEquals;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.jboss.netty.buffer.ChannelBuffers;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import scribe.thrift.LogEntry;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.TestServerStarter;
import com.inmobi.messaging.TestSimple;

public class TestThriftSender {
	private NtMultiServer server;
	private ScribeMessagePublisher mb;
	
	@BeforeTest
	public void setUp() {
		server = TestServerStarter.getServer();
	}
	
	@AfterTest
	public void tearDown()
	{
		server.stop();
		if(mb != null)
			mb.close();
	}
	
	@Test()
	public void simpleSend() throws TException, InterruptedException
	{
		server.start();

		mb = new ScribeMessagePublisher();
		mb.init("ch", TestServerStarter.createConfig("localhost", 
		    TestServerStarter.port));
		TestSimple.waitForConnectComplete(mb);
		TimingAccumulator inspector = mb.getStats();
		
		long success = inspector.getSuccessCount();
		mb.publish(toMessage(getRandomThriftObject()));

		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		assertEquals(inspector.getSuccessCount(), success + 1);
	}

	@Test(timeOut = 10000)
	public void serialBlaster() throws TException, InterruptedException
	{
		final int loop = 100 * 1000;
		
		server.start();

		mb = new ScribeMessagePublisher();
    mb.init("ch", TestServerStarter.createConfig("localhost", 
        TestServerStarter.port));
    TestSimple.waitForConnectComplete(mb);
    TimingAccumulator inspector = mb.getStats();
		
		long success = inspector.getSuccessCount();
		
		for(int i = 0; i < loop; i++)
		{
			mb.publish( toMessage(getRandomThriftObject()) );
		}

		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		
		assertEquals(inspector.getSuccessCount(), success + loop);
	}

	@Test(timeOut = 10000)
	public void throttledSerialBlaster() throws TException, InterruptedException
	{
		final int loop = 100*1000;
		
		server.start();

		mb = new ScribeMessagePublisher();
    mb.init("ch", TestServerStarter.createConfig("localhost", 
        TestServerStarter.port));
    TestSimple.waitForConnectComplete(mb);
    TimingAccumulator inspector = mb.getStats();
		
		long success = inspector.getSuccessCount();
		
		for(int i = 0; i < loop; i++)
		{
			mb.publish( toMessage(getRandomThriftObject()) );
			if( (i & 0x3fff) == 0)
			{
				System.out.println("pacing after " + i);
				Thread.sleep(60);
			}
		}

		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		
		assertEquals(inspector.getSuccessCount(), success + loop);
	}

	@Test(timeOut = 10000)
	public void concurrentSend() throws TException, InterruptedException
	{
		final int loop = 1000;
		final int threadCount = 100;

		Thread t[] = new Thread[threadCount];
		
		server.start();

		mb = new ScribeMessagePublisher();
    mb.init("ch", TestServerStarter.createConfig("localhost", 
        TestServerStarter.port));
    TestSimple.waitForConnectComplete(mb);
    TimingAccumulator inspector = mb.getStats();
		
		long success = inspector.getSuccessCount();
		
		for(int i = 0; i < threadCount; i++)
		{
			t[i] = new Thread()
			{
				@Override
				public void run()
				{
					for(int i = 0; i < loop; i++)
					{
						try {
              mb.publish(toMessage(getRandomThriftObject()) );
            } catch (TException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
					}
				}
			};
		}

		for(int i = 0; i < threadCount; i++)
		{
			t[i].start();
		}

		for(int i = 0; i < threadCount; i++)
		{
			t[i].join();
		}
		
		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		
		assertEquals(inspector.getSuccessCount(), success + loop*threadCount);
	}
	
	private static Message toMessage(TBase thriftOb) throws TException {
	  TNettyChannelBuffer t = new TNettyChannelBuffer(null, ChannelBuffers.dynamicBuffer());
    TProtocol p = new TBinaryProtocol(t);
    thriftOb.write(p);
    return new Message(t.getOutputBuffer().array());
	}

	private static TBase getRandomThriftObject()
	{
		LogEntry e = new LogEntry();
		e.category = "cat";
		e.message = "mmm" + Thread.currentThread().getId();
		return e;
	}
}