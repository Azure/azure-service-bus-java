package com.microsoft.azure.servicebus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.proton.message.Message;

import com.microsoft.azure.servicebus.primitives.ServiceBusException;

final class BrokeredMessageBrowser implements IMessageBrowser
{
	private long lastPeekedSequenceNumber = 0;
	private boolean isReceiveSideBrowser = false;
	private BrokeredMessageReceiver messageReceiver = null;
	private BrokeredMessageSender messageSender = null;
		
	public BrokeredMessageBrowser(BrokeredMessageReceiver messageReceiver)
	{		
		this.messageReceiver = messageReceiver;
		this.isReceiveSideBrowser = true;
	}
	
	public BrokeredMessageBrowser(BrokeredMessageSender messageSender)
	{		
		this.messageSender = messageSender;
		this.isReceiveSideBrowser = false;
	}
	
	@Override
	public IBrokeredMessage peek() throws InterruptedException, ServiceBusException {
		return Utils.completeFuture(this.peekAsync());
	}

	@Override
	public IBrokeredMessage peek(long fromSequenceNumber) throws InterruptedException, ServiceBusException {
		return Utils.completeFuture(this.peekAsync(fromSequenceNumber));
	}

	@Override
	public Collection<IBrokeredMessage> peekBatch(int messageCount) throws InterruptedException, ServiceBusException {
		return Utils.completeFuture(this.peekBatchAsync(messageCount));
	}

	@Override
	public Collection<IBrokeredMessage> peekBatch(long fromSequenceNumber, int messageCount) throws InterruptedException, ServiceBusException {
		return Utils.completeFuture(this.peekBatchAsync(fromSequenceNumber, messageCount));
	}

	@Override
	public CompletableFuture<IBrokeredMessage> peekAsync() {
		return this.peekAsync(this.lastPeekedSequenceNumber + 1);
	}

	@Override
	public CompletableFuture<IBrokeredMessage> peekAsync(long fromSequenceNumber) {
		return this.peekBatchAsync(fromSequenceNumber, 1).thenApply((c) -> 
		{
			IBrokeredMessage message = null;
			Iterator<IBrokeredMessage> iterator = c.iterator();
			if(iterator.hasNext())
			{
				message = iterator.next();
				iterator.remove();
			}
			return message;
		});
	}

	@Override
	public CompletableFuture<Collection<IBrokeredMessage>> peekBatchAsync(int messageCount) {
		return this.peekBatchAsync(this.lastPeekedSequenceNumber + 1, messageCount);
	}

	@Override
	public CompletableFuture<Collection<IBrokeredMessage>> peekBatchAsync(long fromSequenceNumber, int messageCount) {
		CompletableFuture<Collection<Message>> peekFuture;
		if(this.isReceiveSideBrowser)
		{
			String sessionId = this.messageReceiver.isSessionReceiver()? this.messageReceiver.getInternalReceiver().getSessionId() : null;
			peekFuture = this.messageReceiver.getInternalReceiver().peekMessagesAsync(fromSequenceNumber, messageCount, sessionId);
		}
		else
		{
			peekFuture = this.messageSender.getInternalSender().peekMessagesAsync(fromSequenceNumber, messageCount);
		}		
		
		return peekFuture.thenApply((peekedMessages) -> 
		{
			ArrayList<IBrokeredMessage> convertedMessages = new ArrayList<IBrokeredMessage>();
			if(peekedMessages != null)
			{
				long sequenceNumberOfLastMessage = 0;
				for(Message message : peekedMessages)
				{
					BrokeredMessage convertedMessage = MessageConverter.convertAmqpMessageToBrokeredMessage(message);
					sequenceNumberOfLastMessage = convertedMessage.getSequenceNumber();
					convertedMessages.add(convertedMessage);
				}
				
				if(sequenceNumberOfLastMessage > 0)
				{
					this.lastPeekedSequenceNumber = sequenceNumberOfLastMessage;
				}
			}		
			
			return convertedMessages;
		});
	}
}
