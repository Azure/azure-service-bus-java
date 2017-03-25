package com.microsoft.azure.servicebus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public class QueueClientTests {
	private QueueClient queueClient;
	private QueueClient sessionfulQueueClient;
	
	@Before
	public void setUp()
	{		
		
	}
	
	@After
	public void tearDown() throws ServiceBusException, InterruptedException
	{		
		if(this.queueClient != null)
		{
			this.queueClient.close();
			TestCommons.drainAllMessages(TestUtils.getQueueConnectionStringBuilder());
		}
			
		if(this.sessionfulQueueClient != null)
		{
			this.sessionfulQueueClient.close();
			TestCommons.drainAllMessages(TestUtils.getSessionfulQueueConnectionStringBuilder());
		}			
	}
	
	private void createQueueClient() throws InterruptedException, ServiceBusException
	{
		this.queueClient = new QueueClient(TestUtils.getQueueConnectionStringBuilder().toString(), ReceiveMode.PeekLock);
	}
	
	private void createSessionfulQueueClient() throws InterruptedException, ServiceBusException
	{
		this.queueClient = new QueueClient(TestUtils.getSessionfulQueueConnectionStringBuilder().toString(), ReceiveMode.PeekLock);
	}
	
	@Test
	public void testMessagePumpAutoComplete() throws InterruptedException, ServiceBusException
	{
		this.createQueueClient();
		MessagePumpTests.testMessagePumpAutoComplete(this.queueClient, this.queueClient);
	}
	
	@Test
	public void testMessagePumpClientComplete() throws InterruptedException, ServiceBusException
	{
		this.createQueueClient();
		MessagePumpTests.testMessagePumpClientComplete(this.queueClient, this.queueClient);
	}
	
	@Test
	public void testMessagePumpAbandonOnException() throws InterruptedException, ServiceBusException
	{
		this.createQueueClient();
		MessagePumpTests.testMessagePumpAbandonOnException(this.queueClient, this.queueClient);
	}
	
	@Test
	public void testMessagePumpRenewLock() throws InterruptedException, ServiceBusException
	{
		this.createQueueClient();
		MessagePumpTests.testMessagePumpRenewLock(this.queueClient, this.queueClient);
	}
}
