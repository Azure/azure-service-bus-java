package com.microsoft.azure.servicebus;

import java.sql.Date;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.primitives.StringUtil;

public class QueueClient extends InitializableEntity implements IQueueClient
{
	private IMessageSender sender;
	private IMessageReceiver receiver;
	private MessageAndSessionPump messageAndSessionPump;
	private SessionBrowser sessionBrowser;
	
	private QueueClient()
	{
		super(StringUtil.getRandomString(), null);
	}
	
	public QueueClient(String amqpConnectionString, ReceiveMode receiveMode) throws InterruptedException, ServiceBusException
	{
		this();
		ConnectionStringBuilder builder = new ConnectionStringBuilder(amqpConnectionString);
		CompletableFuture<MessagingFactory> factoryFuture = MessagingFactory.createFromConnectionStringBuilderAsync(builder);		
		Utils.completeFuture(factoryFuture.thenComposeAsync((f) -> this.CreateSendersAndReceiversAsync(f, builder.getEntityPath(), receiveMode)));
	}
	
	public QueueClient(MessagingFactory factory, String queuePath, ReceiveMode receiveMode) throws InterruptedException, ServiceBusException
	{
		this();
		Utils.completeFuture(this.CreateSendersAndReceiversAsync(factory, queuePath, receiveMode));
	}
	
	private CompletableFuture<Void> CreateSendersAndReceiversAsync(MessagingFactory factory, String queuePath, ReceiveMode receiveMode)
	{
		CompletableFuture<IMessageSender> senderFuture = ClientFactory.createMessageSenderFromEntityPathAsync(factory, queuePath);
		CompletableFuture<Void> postSenderFuture = senderFuture.thenAcceptAsync((s) -> {
			this.sender = s;			
		});
		CompletableFuture<IMessageReceiver> receiverFuture = ClientFactory.createMessageReceiverFromEntityPathAsync(factory, queuePath, receiveMode);
		CompletableFuture<Void> postRecieverFuture = receiverFuture.thenAcceptAsync((r) -> {
			this.receiver = r;
			this.messageAndSessionPump = new MessageAndSessionPump(this.receiver);
			this.sessionBrowser = new SessionBrowser(factory, (BrokeredMessageReceiver)this.receiver, queuePath);
		});
		
		return CompletableFuture.allOf(postSenderFuture, postRecieverFuture);
	}	
	
	@Override
	public ReceiveMode getReceiveMode() {
		return this.receiver.getReceiveMode();
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
	public void registerMessageHandler(IMessageHandler handler) {
		this.messageAndSessionPump.registerMessageHandler(handler);		
	}

	@Override
	public void registerMessageHandler(IMessageHandler handler, MessageHandlerOptions handlerOptions) {
		this.messageAndSessionPump.registerMessageHandler(handler, handlerOptions);		
	}

	@Override
	public void registerSessionHandler(ISessionHandler handler) {
		this.messageAndSessionPump.registerSessionHandler(handler);		
	}

	@Override
	public void registerSessionHandler(ISessionHandler handler, SessionHandlerOptions handlerOptions) {
		this.messageAndSessionPump.registerSessionHandler(handler, handlerOptions);		
	}

	// No op now
	@Override
	CompletableFuture<Void> initializeAsync() throws Exception {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CompletableFuture<Void> onClose() {
		return this.messageAndSessionPump.closeAsync().thenCompose((v) -> this.sender.closeAsync().thenCompose((u) -> this.receiver.closeAsync()));
	}

	@Override
	public Collection<IMessageSession> getMessageSessions() throws InterruptedException, ServiceBusException {
		return Utils.completeFuture(this.getMessageSessionsAsync());
	}

	@Override
	public Collection<IMessageSession> getMessageSessions(Instant lastUpdatedTime) throws InterruptedException, ServiceBusException {
		return Utils.completeFuture(this.getMessageSessionsAsync(lastUpdatedTime));
	}

	@Override
	public CompletableFuture<Collection<IMessageSession>> getMessageSessionsAsync() {
		return this.sessionBrowser.getMessageSessionsAsync();
	}

	@Override
	public CompletableFuture<Collection<IMessageSession>> getMessageSessionsAsync(Instant lastUpdatedTime) {
		return this.sessionBrowser.getMessageSessionsAsync(Date.from(lastUpdatedTime));
	}

	@Override
	public void abandon(UUID lockToken) throws InterruptedException, ServiceBusException {
		this.messageAndSessionPump.abandon(lockToken);
	}

	@Override
	public void abandon(UUID lockToken, Map<String, Object> propertiesToModify)	throws InterruptedException, ServiceBusException {
		this.messageAndSessionPump.abandon(lockToken, propertiesToModify);		
	}

	@Override
	public CompletableFuture<Void> abandonAsync(UUID lockToken) {
		return this.messageAndSessionPump.abandonAsync(lockToken);
	}

	@Override
	public CompletableFuture<Void> abandonAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
		return this.messageAndSessionPump.abandonAsync(lockToken, propertiesToModify);
	}

	@Override
	public void complete(UUID lockToken) throws InterruptedException, ServiceBusException {
		this.messageAndSessionPump.complete(lockToken);		
	}

	@Override
	public CompletableFuture<Void> completeAsync(UUID lockToken) {
		return this.messageAndSessionPump.completeAsync(lockToken);
	}

	@Override
	public void defer(UUID lockToken) throws InterruptedException, ServiceBusException {
		this.messageAndSessionPump.defer(lockToken);		
	}

	@Override
	public void defer(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
		this.messageAndSessionPump.defer(lockToken, propertiesToModify);		
	}

	@Override
	public CompletableFuture<Void> deferAsync(UUID lockToken) {
		return this.messageAndSessionPump.deferAsync(lockToken);
	}

	@Override
	public CompletableFuture<Void> deferAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
		return this.messageAndSessionPump.deferAsync(lockToken, propertiesToModify);
	}

	@Override
	public void deadLetter(UUID lockToken) throws InterruptedException, ServiceBusException {
		this.messageAndSessionPump.deadLetter(lockToken);		
	}

	@Override
	public void deadLetter(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
		this.messageAndSessionPump.deadLetter(lockToken, propertiesToModify);		
	}

	@Override
	public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription) throws InterruptedException, ServiceBusException {
		this.messageAndSessionPump.deadLetter(lockToken, deadLetterReason, deadLetterErrorDescription);		
	}

	@Override
	public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
		this.messageAndSessionPump.deadLetter(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify);		
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken) {
		return this.messageAndSessionPump.deadLetterAsync(lockToken);
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
		return this.messageAndSessionPump.deadLetterAsync(lockToken, propertiesToModify);
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription) {
		return this.messageAndSessionPump.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription);
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason,	String deadLetterErrorDescription, Map<String, Object> propertiesToModify) {
		return this.messageAndSessionPump.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify);
	}
}
