package com.microsoft.azure.servicebus;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.rules.Filter;
import com.microsoft.azure.servicebus.rules.RuleDescription;

public class SubscriptionClient implements ISubscriptionClient {

	private IMessageReceiver receiver;	
	
	public SubscriptionClient(String amqpConnectionString, ReceiveMode receiveMode) throws InterruptedException, ServiceBusException, IOException
	{
		this.receiver = ClientFactory.createMessageReceiverFromConnectionString(amqpConnectionString, receiveMode);
	}
	
	public SubscriptionClient(MessagingFactory factory, String subscriptionPath, ReceiveMode receiveMode) throws InterruptedException, ServiceBusException, IOException
	{
		this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, subscriptionPath, receiveMode);
	}
	
	@Override
	public ReceiveMode getReceiveMode() {
		return this.receiver.getReceiveMode();
	}

	@Override
	public void abandon(UUID lockToken) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public void abandon(UUID lockToken, Map<String, Object> propertiesToModify)
			throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public CompletableFuture<Void> abandonAsync(UUID lockToken) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Void> abandonAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void complete(UUID lockToken) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public void completeBatch(Collection<? extends IBrokeredMessage> messages) {
		// TODO Auto-generated method stub

	}

	@Override
	public CompletableFuture<Void> completeAsync(UUID lockToken) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Void> completeBatchAsync(Collection<? extends IBrokeredMessage> messages) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void defer(UUID lockToken) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public void defer(UUID lockToken, Map<String, Object> propertiesToModify)
			throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public CompletableFuture<Void> deferAsync(UUID lockToken) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Void> deferAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deadLetter(UUID lockToken) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deadLetter(UUID lockToken, Map<String, Object> propertiesToModify)
			throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription)
			throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription,
			Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason,
			String deadLetterErrorDescription) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason,
			String deadLetterErrorDescription, Map<String, Object> propertiesToModify) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IBrokeredMessage receive() throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IBrokeredMessage receive(Duration serverWaitTime) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IBrokeredMessage receive(long sequenceNumber) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<IBrokeredMessage> receiveBatch(int maxMessageCount)
			throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<IBrokeredMessage> receiveBatch(int maxMessageCount, Duration serverWaitTime)
			throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<IBrokeredMessage> receiveBatch(Collection<Long> sequenceNumbers)
			throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<IBrokeredMessage> receiveAsync() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<IBrokeredMessage> receiveAsync(Duration serverWaitTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<IBrokeredMessage> receiveAsync(long sequenceNumber) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Collection<IBrokeredMessage>> receiveBatchAsync(int maxMessageCount) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Collection<IBrokeredMessage>> receiveBatchAsync(int maxMessageCount,
			Duration serverWaitTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Collection<IBrokeredMessage>> receiveBatchAsync(Collection<Long> sequenceNumbers) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Instant> renewMessageLockAsync(IBrokeredMessage message) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Instant renewMessageLock(IBrokeredMessage message) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMessagePrefetchCount() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public void setMessagePrefetchCount(int prefetchCount) throws ServiceBusException {
		// TODO Auto-generated method stub		
	}
	
	@Override
	public int getSessionPrefetchCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setSessionPrefetchCount(int prefetchCount){
		// TODO Auto-generated method stub

	}

	@Override
	public String getEntityPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws ServiceBusException {
		// TODO Auto-generated method stub

	}

	@Override
	public IBrokeredMessage peek() throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IBrokeredMessage peek(long fromSequenceNumber) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<IBrokeredMessage> peekBatch(int messageCount) throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<IBrokeredMessage> peekBatch(long fromSequenceNumber, int messageCount)
			throws InterruptedException, ServiceBusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<IBrokeredMessage> peekAsync() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<IBrokeredMessage> peekAsync(long fromSequenceNumber) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Collection<IBrokeredMessage>> peekBatchAsync(int messageCount) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Collection<IBrokeredMessage>> peekBatchAsync(long fromSequenceNumber, int messageCount) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMessageSession acceptMessageSession() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMessageSession acceptMessageSession(Duration serverWaitTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMessageSession acceptMessageSession(String sessionId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMessageSession acceptMessageSession(String sessionId, Duration serverWaitTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<IMessageSession> getMessageSessions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<IMessageSession> getMessageSessions(Instant lastUpdatedTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<IMessageSession> acceptMessageSessionAsync() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<IMessageSession> acceptMessageSessionAsync(Duration serverWaitTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<IMessageSession> acceptMessageSessionAsync(String sessionId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<IMessageSession> acceptMessageSessionAsync(String sessionId, Duration serverWaitTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Iterable<IMessageSession>> getMessageSessionsAsync() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Iterable<IMessageSession>> getMessageSessionsAsync(Instant lastUpdatedTime) {
		// TODO Auto-generated method stub
		return null;
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
}
