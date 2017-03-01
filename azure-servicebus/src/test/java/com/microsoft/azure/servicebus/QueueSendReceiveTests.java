package com.microsoft.azure.servicebus;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.MessageNotFoundException;
import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public class QueueSendReceiveTests {
	private ConnectionStringBuilder builder;
	private MessagingFactory factory;
	private IMessageSender sender;
	private IMessageReceiver receiver;
	private IMessageBrowser browser;
	private final String sessionId = null;
	
	@Before // Fix this. something goes wrong when we do this setup.
	public void setup() throws IOException, InterruptedException, ExecutionException, ServiceBusException
	{
		this.builder = TestUtils.getQueueConnectionStringBuilder();
		this.factory = MessagingFactory.createFromConnectionStringBuilder(builder);
		this.sender = ClientFactory.createMessageSenderFromConnectionStringBuilder(builder);		
		
		this.drainAllMessages(builder);
		//Thread.sleep(60000);
	}
	
	@After
	public void tearDown() throws ServiceBusException
	{
		this.sender.close();
		if(this.receiver != null)
			this.receiver.close();
		if(this.browser != null)
			this.browser.close();
		this.factory.close();
	}
	
	@Test
	public void testBasicQueueSend() throws InterruptedException, ServiceBusException, IOException
	{		
		TestCommons.testBasicQueueSend(this.sender);
	}
	
	@Test
	public void testBasicQueueSendBatch() throws InterruptedException, ServiceBusException, IOException
	{
		TestCommons.testBasicQueueSendBatch(this.sender);
	}
	
	@Test
	public void testBasicQueueReceiveAndDelete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, builder.getEntityPath(), ReceiveMode.ReceiveAndDelete);
		TestCommons.testBasicQueueReceiveAndDelete(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testBasicQueueReceiveBatchAndDelete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, builder.getEntityPath(), ReceiveMode.ReceiveAndDelete);
		TestCommons.testBasicQueueReceiveBatchAndDelete(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testBasicQueueReceiveAndComplete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, builder.getEntityPath(), ReceiveMode.PeekLock);

		TestCommons.testBasicQueueReceiveAndComplete(this.sender, this.sessionId, this.receiver);

	}
	
	@Test
	public void testBasicQueueReceiveAndAbandon() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, builder.getEntityPath(), ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndAbandon(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testBasicQueueReceiveAndDeadLetter() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, builder.getEntityPath(), ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndDeadLetter(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testBasicQueueReceiveAndRenewLock() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, builder.getEntityPath(), ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndRenewLock(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testBasicQueueReceiveAndRenewLockBatch() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{		
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, builder.getEntityPath(), ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveAndRenewLockBatch(this.sender, this.sessionId, this.receiver);

	}
	
	@Test
	public void testBasicQueueReceiveBatchAndComplete() throws InterruptedException, ServiceBusException, IOException, ExecutionException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, builder.getEntityPath(), ReceiveMode.PeekLock);
		TestCommons.testBasicQueueReceiveBatchAndComplete(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testSendSceduledMessageAndReceive() throws InterruptedException, ServiceBusException, IOException
	{	
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.ReceiveAndDelete);
		TestCommons.testSendSceduledMessageAndReceive(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testSendSceduledMessageAndCancel() throws InterruptedException, ServiceBusException, IOException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.ReceiveAndDelete);
		TestCommons.testSendSceduledMessageAndCancel(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testPeekMessage() throws InterruptedException, ServiceBusException, IOException
	{		
		this.browser = ClientFactory.createMessageBrowserFromEntityPath(factory, this.builder.getEntityPath());
		TestCommons.testPeekMessage(this.sender, this.sessionId, this.browser);
	}
	
	@Test
	public void testPeekMessageBatch() throws InterruptedException, ServiceBusException, IOException
	{
		this.browser = ClientFactory.createMessageBrowserFromEntityPath(factory, this.builder.getEntityPath());
		TestCommons.testPeekMessageBatch(this.sender, this.sessionId, this.browser);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndComplete() throws InterruptedException, ServiceBusException, IOException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndComplete(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndAbandon() throws InterruptedException, ServiceBusException, IOException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndAbandon(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndDefer() throws InterruptedException, ServiceBusException, IOException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndDefer(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndDeadletter() throws InterruptedException, ServiceBusException, IOException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.PeekLock);
		TestCommons.testReceiveBySequenceNumberAndDeadletter(this.sender, this.sessionId, this.receiver);
	}
	
	@Test
	public void testReceiveBySequenceNumberAndComplete() throws InterruptedException, ServiceBusException, IOException
	{			
		this.sender.send(new BrokeredMessage("AMQP message"));
		
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.PeekLock);
		IBrokeredMessage receivedMessage = this.receiver.receive();
		long sequenceNumber = receivedMessage.getSequenceNumber();
		String messageId = receivedMessage.getMessageId();
		this.receiver.defer(receivedMessage.getLockToken());		
		
		// Now receive by sequence number
		receivedMessage = this.receiver.receive(sequenceNumber);
		Assert.assertEquals("ReceiveBySequenceNumber didn't receive the right message.", sequenceNumber, receivedMessage.getSequenceNumber());
		Assert.assertEquals("ReceiveBySequenceNumber didn't receive the right message.", messageId, receivedMessage.getMessageId());		
		this.receiver.complete(receivedMessage.getLockToken());
		
		// Try to receive by sequence number again
		try
		{
			receivedMessage = this.receiver.receive(sequenceNumber);
			Assert.fail("Message recieved by sequnce number was not properly completed.");
		}
		catch(MessageNotFoundException e)
		{
			// Expected
		}		
	}
	
	@Test
	public void testReceiveBySequenceNumberAndAbandon() throws InterruptedException, ServiceBusException, IOException
	{			
		this.sender.send(new BrokeredMessage("AMQP message"));
		
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.PeekLock);
		IBrokeredMessage receivedMessage = this.receiver.receive();
		long sequenceNumber = receivedMessage.getSequenceNumber();
		String messageId = receivedMessage.getMessageId();
		this.receiver.defer(receivedMessage.getLockToken());		
		
		// Now receive by sequence number
		receivedMessage = this.receiver.receive(sequenceNumber);
		Assert.assertEquals("ReceiveBySequenceNumber didn't receive the right message.", sequenceNumber, receivedMessage.getSequenceNumber());
		Assert.assertEquals("ReceiveBySequenceNumber didn't receive the right message.", messageId, receivedMessage.getMessageId());
		long deliveryCount = receivedMessage.getDeliveryCount();
		this.receiver.abandon(receivedMessage.getLockToken());
		
		// Try to receive by sequence number again
		receivedMessage = this.receiver.receive(sequenceNumber);
		Assert.assertEquals("Abandon didn't increase the delivery count for the message received by sequence number.", deliveryCount + 1, receivedMessage.getDeliveryCount());
		this.receiver.complete(receivedMessage.getLockToken());		
	}
	
	@Test
	public void testReceiveBySequenceNumberAndDefer() throws InterruptedException, ServiceBusException, IOException
	{			
		BrokeredMessage sentMessage = new BrokeredMessage("AMQP message");
		HashMap customProperties = new HashMap();
		customProperties.put("phase", "before defer");
		sentMessage.setProperties(customProperties);
		this.sender.send(sentMessage);
		
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.PeekLock);
		IBrokeredMessage receivedMessage = this.receiver.receive();
		long sequenceNumber = receivedMessage.getSequenceNumber();
		String messageId = receivedMessage.getMessageId();
		this.receiver.defer(receivedMessage.getLockToken());	
		
		// Now receive by sequence number
		receivedMessage = this.receiver.receive(sequenceNumber);
		Assert.assertEquals("ReceiveBySequenceNumber didn't receive the right message.", sequenceNumber, receivedMessage.getSequenceNumber());
		Assert.assertEquals("ReceiveBySequenceNumber didn't receive the right message.", messageId, receivedMessage.getMessageId());		
		customProperties.put("phase", "after defer");
		this.receiver.defer(receivedMessage.getLockToken(), customProperties);
		
		// Try to receive by sequence number again
		receivedMessage = this.receiver.receive(sequenceNumber);
		Assert.assertEquals("ReceiveBySequenceNumber didn't receive the right message after deferrring", sequenceNumber, receivedMessage.getSequenceNumber());
		Assert.assertEquals("Defer didn't update properties of the message received by sequence number", "after defer", receivedMessage.getProperties().get("phase"));
		this.receiver.complete(receivedMessage.getLockToken());		
	}
	
	@Test
	public void testReceiveBySequenceNumberAndDeadletter() throws InterruptedException, ServiceBusException, IOException
	{			
		this.sender.send(new BrokeredMessage("AMQP message"));
		
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.builder.getEntityPath(), ReceiveMode.PeekLock);
		IBrokeredMessage receivedMessage = this.receiver.receive();
		long sequenceNumber = receivedMessage.getSequenceNumber();
		String messageId = receivedMessage.getMessageId();
		this.receiver.defer(receivedMessage.getLockToken());		
		
		// Now receive by sequence number
		receivedMessage = this.receiver.receive(sequenceNumber);
		Assert.assertEquals("ReceiveBySequenceNumber didn't receive the right message.", sequenceNumber, receivedMessage.getSequenceNumber());
		Assert.assertEquals("ReceiveBySequenceNumber didn't receive the right message.", messageId, receivedMessage.getMessageId());
		String deadLetterReason = "java client deadletter test";		
		this.receiver.deadLetter(receivedMessage.getLockToken(), deadLetterReason, null);
				
		// Try to receive by sequence number again
		try
		{
			receivedMessage = this.receiver.receive(sequenceNumber);
			Assert.fail("Message received by sequence number was not properly deadlettered");
		}
		catch(MessageNotFoundException e)
		{
			// Expected
		}
	}
	
	private void drainAllMessages(ConnectionStringBuilder builder) throws IOException, InterruptedException, ExecutionException, ServiceBusException
	{
		Duration waitTime = Duration.ofSeconds(5);
		final int batchSize = 10;		
		IMessageReceiver receiver = ClientFactory.createMessageReceiverFromEntityPath(this.factory, this.builder.getEntityPath(), ReceiveMode.ReceiveAndDelete);
		Collection<IBrokeredMessage> messages = receiver.receiveBatch(batchSize, waitTime);
		while(messages !=null && messages.size() > 0)
		{
			messages = receiver.receiveBatch(batchSize, waitTime);
		}
		
		IMessageBrowser browser = ClientFactory.createMessageBrowserFromEntityPath(this.factory, this.builder.getEntityPath());
		IBrokeredMessage message;
		while((message = browser.peek()) != null)
		{
			receiver.receive(message.getSequenceNumber());
		}
		
		browser.close();
		receiver.close();
	}	
}
