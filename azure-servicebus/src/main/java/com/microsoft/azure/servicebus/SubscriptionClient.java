package com.microsoft.azure.servicebus;

import java.sql.Date;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.primitives.StringUtil;
import com.microsoft.azure.servicebus.rules.Filter;
import com.microsoft.azure.servicebus.rules.RuleDescription;

public class SubscriptionClient extends InitializableEntity implements ISubscriptionClient {

	private IMessageReceiver receiver;
	private MessageAndSessionPump messageAndSessionPump;
	private SessionBrowser sessionBrowser;
	
	private SubscriptionClient()
	{
		super(StringUtil.getRandomString(), null);
	}
	
	public SubscriptionClient(String amqpConnectionString, ReceiveMode receiveMode) throws InterruptedException, ServiceBusException
	{
		this();
		this.receiver = ClientFactory.createMessageReceiverFromConnectionString(amqpConnectionString, receiveMode);
		this.createPumpAndBrowser();
	}
	
	public SubscriptionClient(MessagingFactory factory, String subscriptionPath, ReceiveMode receiveMode) throws InterruptedException, ServiceBusException
	{
		this();
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, subscriptionPath, receiveMode);
		this.createPumpAndBrowser();
	}
	
	private void createPumpAndBrowser()
	{
		this.messageAndSessionPump = new MessageAndSessionPump(this.receiver);
		this.sessionBrowser = new SessionBrowser(((BrokeredMessageReceiver)this.receiver).getMessagingFactory(), (BrokeredMessageReceiver)this.receiver, ((BrokeredMessageReceiver)this.receiver).getEntityPath());
	}
	
	@Override
	public ReceiveMode getReceiveMode() {
		return this.receiver.getReceiveMode();
	}
	
	@Override
	public String getEntityPath() {
		return this.receiver.getEntityPath();
	}

	@Override
	public void addRule(RuleDescription ruleDescription) throws InterruptedException, ServiceBusException {
		Utils.completeFuture(this.addRuleAsync(ruleDescription));
	}

	@Override
	public CompletableFuture<Void> addRuleAsync(RuleDescription ruleDescription) {
		return ((BrokeredMessageReceiver)this.receiver).getInternalReceiver().addRuleAsync(ruleDescription);
	}

	@Override
	public void addRule(String ruleName, Filter filter) throws InterruptedException, ServiceBusException {
		Utils.completeFuture(this.addRuleAsync(ruleName, filter));
	}

	@Override
	public CompletableFuture<Void> addRuleAsync(String ruleName, Filter filter) {
		return this.addRuleAsync(new RuleDescription(ruleName, filter));
	}
	
	@Override
	public void removeRule(String ruleName) throws InterruptedException, ServiceBusException {
		Utils.completeFuture(this.removeRuleAsync(ruleName));
	}

	@Override
	public CompletableFuture<Void> removeRuleAsync(String ruleName) {
		return ((BrokeredMessageReceiver)this.receiver).getInternalReceiver().removeRuleAsync(ruleName);
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
		return this.messageAndSessionPump.closeAsync().thenCompose((v) -> this.receiver.closeAsync());
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
