package com.microsoft.azure.servicebus;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.servicebus.primitives.ServiceBusException;

import org.junit.Assert;

public class MessagePumpTests {
	private static final int DEFAULT_MAX_CONCURRENT_CALLS = 5;
	
	public static void testMessagePumpAutoComplete(IMessageSender sender, IMessageAndSessionPump messagePump) throws InterruptedException, ServiceBusException
	{
		int numMessages = 10;
		for(int i=0; i<numMessages; i++)
		{
			sender.send(new BrokeredMessage("AMQPMessage"));
		}
		boolean autoComplete = true;
		CountingMessageHandler messageHandler = new CountingMessageHandler(messagePump, !autoComplete, numMessages, false);		
		messagePump.registerMessageHandler(messageHandler, new MessageHandlerOptions(DEFAULT_MAX_CONCURRENT_CALLS, autoComplete, Duration.ofMinutes(10)));
		if(!messageHandler.getMessageCountDownLactch().await(2, TimeUnit.MINUTES))
		{
			Assert.fail("All messages not pumped even after waiting for 2 minutes.");
		}
		
		Assert.assertTrue("OnMessage called by maximum of one concurrent thread.", messageHandler.getMaxConcurrencyCounter().getMaxConcurrencyCount() > 1);
		Assert.assertTrue("OnMessage called by more than maxconcurrentcalls threads.", messageHandler.getMaxConcurrencyCounter().getMaxConcurrencyCount() <= DEFAULT_MAX_CONCURRENT_CALLS);
	}
	
	
	public static void testMessagePumpClientComplete(IMessageSender sender, IMessageAndSessionPump messagePump) throws InterruptedException, ServiceBusException
	{
		int numMessages = 10;
		for(int i=0; i<numMessages; i++)
		{
			sender.send(new BrokeredMessage("AMQPMessage"));
		}
		boolean autoComplete = false;
		CountingMessageHandler messageHandler = new CountingMessageHandler(messagePump, !autoComplete, numMessages, false);		
		messagePump.registerMessageHandler(messageHandler, new MessageHandlerOptions(DEFAULT_MAX_CONCURRENT_CALLS, autoComplete, Duration.ofMinutes(10)));
		if(!messageHandler.getMessageCountDownLactch().await(2, TimeUnit.MINUTES))
		{
			Assert.fail("All messages not pumped even after waiting for 2 minutes.");
		}
		Assert.assertTrue("OnMessage called by maximum of one concurrent thread.", messageHandler.getMaxConcurrencyCounter().getMaxConcurrencyCount() > 1);
		Assert.assertTrue("OnMessage called by more than maxconcurrentcalls threads.", messageHandler.getMaxConcurrencyCounter().getMaxConcurrencyCount() <= DEFAULT_MAX_CONCURRENT_CALLS);
	}
	public static void testMessagePumpAbandonOnException(IMessageSender sender, IMessageAndSessionPump messagePump) throws InterruptedException, ServiceBusException
	{
		int numMessages = 10;
		for(int i=0; i<numMessages; i++)
		{
			sender.send(new BrokeredMessage("AMQPMessage"));
		}
		boolean autoComplete = false;
		CountingMessageHandler messageHandler = new CountingMessageHandler(messagePump, !autoComplete, numMessages, true);		
		messagePump.registerMessageHandler(messageHandler, new MessageHandlerOptions(DEFAULT_MAX_CONCURRENT_CALLS, autoComplete, Duration.ofMinutes(10)));
		if(!messageHandler.getMessageCountDownLactch().await(2, TimeUnit.MINUTES))
		{
			Assert.fail("All messages not pumped even after waiting for 2 minutes.");
		}
		Assert.assertTrue("OnMessage called by maximum of one concurrent thread.", messageHandler.getMaxConcurrencyCounter().getMaxConcurrencyCount() > 1);
		Assert.assertTrue("OnMessage called by more than maxconcurrentcalls threads.", messageHandler.getMaxConcurrencyCounter().getMaxConcurrencyCount() <= DEFAULT_MAX_CONCURRENT_CALLS);
	}
	
	public static void testMessagePumpRenewLock(IMessageSender sender, IMessageAndSessionPump messagePump) throws InterruptedException, ServiceBusException
	{
		int numMessages = 5;
		for(int i=0; i<numMessages; i++)
		{
			sender.send(new BrokeredMessage("AMQPMessage"));
		}
		boolean autoComplete = true;
		int sleepMinutes = 1; // This should be less than message lock duration of the queue or subscription
		CountingMessageHandler messageHandler = new CountingMessageHandler(messagePump, !autoComplete, numMessages, false, Duration.ofMinutes(sleepMinutes));		
		messagePump.registerMessageHandler(messageHandler, new MessageHandlerOptions(numMessages, autoComplete, Duration.ofMinutes(10)));
		if(!messageHandler.getMessageCountDownLactch().await(2 * sleepMinutes, TimeUnit.MINUTES))
		{
			Assert.fail("All messages not pumped even after waiting for " + (2 * sleepMinutes) + " minutes.");
		}
		Assert.assertTrue("OnMessage called by maximum of one concurrent thread.", messageHandler.getMaxConcurrencyCounter().getMaxConcurrencyCount() > 1);
		Assert.assertTrue("OnMessage called by more than maxconcurrentcalls threads.", messageHandler.getMaxConcurrencyCounter().getMaxConcurrencyCount() <= numMessages);
	}
	
	
	private static class CountingMessageHandler extends TestMessageHandler
	{
		private IMessageAndSessionPump messagePump;
		private boolean completeMessage;
		private CountDownLatch messageCountDownLatch;
		private int messageCount;
		private boolean firstThrowException;
		private Duration sleepDuration;
		private MaxConcurrencyCounter maxConcurrencyCounter;
		
		CountingMessageHandler(IMessageAndSessionPump messagePump, boolean completeMessages, int messageCount, boolean firstThrowException)
		{
			this(messagePump, completeMessages, messageCount, firstThrowException, Duration.ZERO);
		}
		
		CountingMessageHandler(IMessageAndSessionPump messagePump, boolean completeMessages, int messageCount, boolean firstThrowException, Duration sleepDuration)
		{
			this.maxConcurrencyCounter = new MaxConcurrencyCounter(); 
			this.messagePump = messagePump;
			this.completeMessage = completeMessages;
			this.messageCount = messageCount;
			this.firstThrowException = firstThrowException;
			this.sleepDuration = sleepDuration;
			
			if(firstThrowException)
			{
				this.messageCountDownLatch = new CountDownLatch(messageCount * 2);
			}
			else
			{
				this.messageCountDownLatch = new CountDownLatch(messageCount);
			}			
		}
		
		@Override
		public CompletableFuture<Void> onMessageAsync(IBrokeredMessage message) {
			CompletableFuture<Void> countingFuture = CompletableFuture.runAsync(() -> {
				this.maxConcurrencyCounter.incrementCount();
				System.out.println("Message Received - " + message.getMessageId() + " - delivery count:" + message.getDeliveryCount() + " - Thread:" + Thread.currentThread());				
				if(this.firstThrowException && message.getDeliveryCount() == 0)
				{
					this.messageCountDownLatch.countDown();
					this.maxConcurrencyCounter.decrementCount();
					throw new RuntimeException("Dummy exception to cause abandon");
				}
				
				if(!this.sleepDuration.isZero())
				{
					try {
						Thread.sleep(this.sleepDuration.toMillis());
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					};
				}				
			});
			
			CompletableFuture<Void> completeFuture = countingFuture;
			if(this.completeMessage)
			{
				completeFuture = countingFuture.thenCompose((v) -> this.messagePump.completeAsync(message.getLockToken()));
			}
			else
			{
				completeFuture = countingFuture;
			}
			
			return completeFuture.thenRun(() -> {
				this.messageCountDownLatch.countDown();
				this.maxConcurrencyCounter.decrementCount();
				});
		}
		
		public CountDownLatch getMessageCountDownLactch()
		{
			return this.messageCountDownLatch;
		}
		
		public MaxConcurrencyCounter getMaxConcurrencyCounter()
		{
			return this.maxConcurrencyCounter;
		}
	}	
}
