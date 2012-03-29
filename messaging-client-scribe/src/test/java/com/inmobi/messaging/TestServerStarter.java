package com.inmobi.messaging;

import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import random.pkg.NtMultiServer;

public class TestServerStarter {
	private static NtMultiServer server;

	@BeforeSuite
	public void setUp()
	{
		safeInit();
	}

	@AfterSuite
	public void tearDown()
	{
		server.stop();
	}
	
	public static NtMultiServer getServer()
	{
		safeInit();
		assertNotNull(server);
		return server;
	}

	private static synchronized void safeInit()
	{
		if(server == null)
		{
			server = new NtMultiServer();	
		}
	}
	
	public static final int port = 7912;
	
	public static ClientConfig createConfig(String host, int port) {
	  Map<String, String> props = new HashMap<String, String>();
    props.put("host", host);
    props.put("port", String.valueOf(port));
    ClientConfig config = new ClientConfig(props);
    return config;
	}
	
}
