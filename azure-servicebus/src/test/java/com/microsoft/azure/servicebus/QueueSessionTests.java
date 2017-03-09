package com.microsoft.azure.servicebus;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.primitives.TimeoutException;

public class QueueSessionTests {
	private ConnectionStringBuilder builder;
	private MessagingFactory factory;
	private IMessageSender sender;
	private IMessageSession session;
	
	@Before // Fix this. something goes wrong when we do this setup.
	public void setup() throws IOException, InterruptedException, ExecutionException, ServiceBusException
	{
		this.builder = TestUtils.getSessionfulQueueConnectionStringBuilder();
		this.factory = MessagingFactory.createFromConnectionStringBuilder(builder);
		this.sender = ClientFactory.createMessageSenderFromConnectionStringBuilder(builder);
	}
	
	private static String getRandomString()
	{
		return UUID.randomUUID().toString();
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
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), sessionId, ReceiveMode.ReceiveAndDelete);
		TestCommons.testBasicQueueReceiveAndDelete(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveBatchAndDelete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), sessionId, ReceiveMode.ReceiveAndDelete);
		TestCommons.testBasicQueueReceiveBatchAndDelete(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndComplete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndComplete(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndAbandon() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndAbandon(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndDeadLetter() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndDeadLetter(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndRenewLock() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndRenewLock(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveAndRenewLockBatch() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndRenewLockBatch(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testBasicQueueReceiveBatchAndComplete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveBatchAndComplete(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testSendSceduledMessageAndReceive() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.ReceiveAndDelete);
		TestCommons.testSendSceduledMessageAndReceive(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testSendSceduledMessageAndCancel() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.ReceiveAndDelete);
		TestCommons.testSendSceduledMessageAndCancel(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testPeekMessage() throws InterruptedException, ServiceBusException, IOException
	{		
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testPeekMessage(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testPeekMessageBatch() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testPeekMessageBatch(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndComplete() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndComplete(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndAbandon() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndAbandon(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndDefer() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndDefer(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndDeadletter() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndDeadletter(this.sender, sessionId, this.session);
	}
	
	@Test
	public void testAcceptAnySession() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		String messageId = getRandomString();
		BrokeredMessage message = new BrokeredMessage("AMQP message");
		message.setMessageId(messageId);
		if(sessionId != null)
		{
			message.setSessionId(sessionId);
		}
		sender.send(message);
		
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), null, ReceiveMode.PeekLock);
		Assert.assertNotNull("Did not receive a session", this.session);		
	}
	
	@Test
	public void testRenewSessionLock() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		Instant initialValidity = this.session.getLockedUntilUtc();
		this.session.renewLock();
		Instant renewedValidity = this.session.getLockedUntilUtc();
		Assert.assertTrue("RenewSessionLock did not renew session lockeduntil time.", renewedValidity.isAfter(initialValidity));
		this.session.renewLock();
		Instant renewedValidity2 = this.session.getLockedUntilUtc();
		Assert.assertTrue("RenewSessionLock did not renew session lockeduntil time.", renewedValidity2.isAfter(renewedValidity));
	}
	
	// TODO Write session state tests
	@Test
	public void testGetAndSetState() throws InterruptedException, ServiceBusException, IOException
	{
		String sessionId = getRandomString();
		this.session = ClientFactory.acceptSessionFromEntityPath(factory, this.builder.getEntityPath(), sessionId, ReceiveMode.PeekLock);
		byte[] initialState = this.session.getState();
		Assert.assertNull("Session state is not null for a new session", initialState);
		byte[] customState = "Custom Session State".getBytes();
		this.session.setState(customState);
		byte[] updatedState = this.session.getState();
		Assert.assertArrayEquals("Session state not updated properly", customState, updatedState);
		this.session.setState(null);
		updatedState = this.session.getState();
		Assert.assertNull("Session state is not removed by setting a null state", updatedState);
		this.session.setState(customState);
		updatedState = this.session.getState();
		Assert.assertArrayEquals("Session state not updated properly", customState, updatedState);
	}
}
