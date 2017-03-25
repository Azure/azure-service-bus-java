package com.microsoft.azure.servicebus;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.primitives.StringUtil;

public final class TopicClient extends InitializableEntity implements ITopicClient
{
	private IMessageSender sender;
	private BrokeredMessageBrowser browser;
	
	private TopicClient()
	{
		super(StringUtil.getRandomString(), null);
	}
	
	public TopicClient(String amqpConnectionString) throws InterruptedException, ServiceBusException
	{
		this();
		this.sender = ClientFactory.createMessageSenderFromConnectionString(amqpConnectionString);
		this.browser = new BrokeredMessageBrowser((BrokeredMessageSender)sender);
	}
	
	public TopicClient(MessagingFactory factory, String topicPath) throws InterruptedException, ServiceBusException
	{
		this();
		this.sender = ClientFactory.createMessageSenderFromEntityPath(factory, topicPath);
		this.browser = new BrokeredMessageBrowser((BrokeredMessageSender)sender);
	}
	
	@Override
	public void send(IBrokeredMessage message) throws InterruptedException, ServiceBusException {
		this.sender.send(message);
	}

	@Override
	public void sendBatch(Collection<? extends IBrokeredMessage> messages) throws InterruptedException, ServiceBusException {
		this.sender.sendBatch(messages);
	}

	@Override
	public CompletableFuture<Void> sendAsync(IBrokeredMessage message) {
		return this.sender.sendAsync(message);
	}

	@Override
	public CompletableFuture<Void> sendBatchAsync(Collection<? extends IBrokeredMessage> messages) {
		return this.sender.sendBatchAsync(messages);
	}

	@Override
	public CompletableFuture<Long> scheduleMessageAsync(IBrokeredMessage message, Instant scheduledEnqueueTimeUtc) {
		return this.sender.scheduleMessageAsync(message, scheduledEnqueueTimeUtc);
	}

	@Override
	public CompletableFuture<Void> cancelScheduledMessageAsync(long sequenceNumber) {
		return this.sender.cancelScheduledMessageAsync(sequenceNumber);
	}

	@Override
	public long scheduleMessage(IBrokeredMessage message, Instant scheduledEnqueueTimeUtc) throws InterruptedException, ServiceBusException {
		return this.sender.scheduleMessage(message, scheduledEnqueueTimeUtc);
	}

	@Override
	public void cancelScheduledMessage(long sequenceNumber) throws InterruptedException, ServiceBusException {
		this.sender.cancelScheduledMessage(sequenceNumber);
	}

	@Override
	public String getEntityPath() {
		return this.sender.getEntityPath();
	}

	@Override
	public IBrokeredMessage peek() throws InterruptedException, ServiceBusException {
		return this.browser.peek();
	}

	@Override
	public IBrokeredMessage peek(long fromSequenceNumber) throws InterruptedException, ServiceBusException {
		return this.browser.peek(fromSequenceNumber);
	}

	@Override
	public Collection<IBrokeredMessage> peekBatch(int messageCount) throws InterruptedException, ServiceBusException {
		return this.browser.peekBatch(messageCount);
	}

	@Override
	public Collection<IBrokeredMessage> peekBatch(long fromSequenceNumber, int messageCount) throws InterruptedException, ServiceBusException {
		return this.browser.peekBatch(fromSequenceNumber, messageCount);
	}

	@Override
	public CompletableFuture<IBrokeredMessage> peekAsync() {
		return this.browser.peekAsync();
	}

	@Override
	public CompletableFuture<IBrokeredMessage> peekAsync(long fromSequenceNumber) {
		return this.browser.peekAsync(fromSequenceNumber);
	}

	@Override
	public CompletableFuture<Collection<IBrokeredMessage>> peekBatchAsync(int messageCount) {
		return this.browser.peekBatchAsync(messageCount);
	}

	@Override
	public CompletableFuture<Collection<IBrokeredMessage>> peekBatchAsync(long fromSequenceNumber, int messageCount) {
		return this.browser.peekBatchAsync(fromSequenceNumber, messageCount);
	}

	// No Op now
	@Override
	CompletableFuture<Void> initializeAsync() throws Exception {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CompletableFuture<Void> onClose() {
		return this.sender.closeAsync();
	}
}
