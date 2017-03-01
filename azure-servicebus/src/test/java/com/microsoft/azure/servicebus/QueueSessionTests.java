package com.microsoft.azure.servicebus;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public class QueueSessionTests {
	private ConnectionStringBuilder builder;
	private MessagingFactory factory;
	private IMessageSender sender;
	private IMessageSession session;
	private final String sessionId = "testSession1";
	
	@Before // Fix this. something goes wrong when we do this setup.
	public void setup() throws IOException, InterruptedException, ExecutionException, ServiceBusException
	{
		this.builder = TestUtils.getSessionfulQueueConnectionStringBuilder();
		this.factory = MessagingFactory.createFromConnectionStringBuilder(builder);
		this.sender = ClientFactory.createMessageSenderFromConnectionStringBuilder(builder);		
		
		// Create session on the entity
		String messageId = UUID.randomUUID().toString();
		BrokeredMessage message = new BrokeredMessage("AMQP message");
		message.setMessageId(messageId);
		if(sessionId != null)
		{
			message.setSessionId(sessionId);
		}
		sender.send(message);
		
		this.drainAllMessages(builder);
	}
	
	@After
	public void tearDown() throws ServiceBusException
	{
		this.sender.close();
		if(this.session != null)
			this.session.close();		
		this.factory.close();
	}
	
	@Test
	public void testBasicQueueReceiveAndDelete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{		
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), this.sessionId, ReceiveMode.ReceiveAndDelete);
		TestCommons.testBasicQueueReceiveAndDelete(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveBatchAndDelete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), this.sessionId, ReceiveMode.ReceiveAndDelete);
		TestCommons.testBasicQueueReceiveBatchAndDelete(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndComplete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndComplete(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndAbandon() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndAbandon(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndDeadLetter() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndDeadLetter(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndRenewLock() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndRenewLock(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndRenewLockBatch() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{		
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndRenewLockBatch(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveBatchAndComplete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveBatchAndComplete(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testSendSceduledMessageAndReceive() throws InterruptedException, ServiceBusException, IOException
	{	
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), this.sessionId, ReceiveMode.ReceiveAndDelete);
		TestCommons.testSendSceduledMessageAndReceive(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testSendSceduledMessageAndCancel() throws InterruptedException, ServiceBusException, IOException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), this.sessionId, ReceiveMode.ReceiveAndDelete);
		TestCommons.testSendSceduledMessageAndCancel(this.sender, this.sessionId, this.session);
	}
	
//	@Test
//	public void testPeekMessage() throws InterruptedException, ServiceBusException, IOException
//	{		
//		this.browser = ClientFactory.createMessageBrowserFromEntityPath(factory, this.builder.getEntityPath());
//		TestCommons.testPeekMessage(this.sender, this.sessionId, this.browser);
//	}
//	
//	@Test
//	public void testPeekMessageBatch() throws InterruptedException, ServiceBusException, IOException
//	{
//		this.browser = ClientFactory.createMessageBrowserFromEntityPath(factory, this.builder.getEntityPath());
//		TestCommons.testPeekMessageBatch(this.sender, this.sessionId, this.browser);
//	}
	
	@Test
	public void testReceiveBySequenceNumberAndComplete() throws InterruptedException, ServiceBusException, IOException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndComplete(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndAbandon() throws InterruptedException, ServiceBusException, IOException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndAbandon(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndDefer() throws InterruptedException, ServiceBusException, IOException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndDefer(this.sender, this.sessionId, this.session);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndDeadletter() throws InterruptedException, ServiceBusException, IOException
	{
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), this.sessionId, ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndDeadletter(this.sender, this.sessionId, this.session);
	}
	
	private void drainAllMessages(ConnectionStringBuilder builder) throws IOException, InterruptedException, ExecutionException, ServiceBusException
	{
		Duration waitTime = Duration.ofSeconds(5);
		final int batchSize = 10;		
		IMessageSession session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), this.sessionId, ReceiveMode.ReceiveAndDelete);
		Collection<IBrokeredMessage> messages = session.receiveBatch(batchSize, waitTime);
		while(messages !=null && messages.size() > 0)
		{
			messages = session.receiveBatch(batchSize, waitTime);
		}	
		
		session.close();
	}
}
