package com.microsoft.azure.servicebus;

import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.primitives.StringUtil;

public class BrokeredMessageSession extends BrokeredMessageReceiver implements IMessageSession
{
	private String requestedSessionId;
	
	BrokeredMessageSession(ConnectionStringBuilder amqpConnectionStringBuilder, String requestedSessionId, ReceiveMode receiveMode)
	{
		super(amqpConnectionStringBuilder, receiveMode);
		this.requestedSessionId = requestedSessionId;
	}
	
	BrokeredMessageSession(MessagingFactory messagingFactory, String entityPath, String requestedSessionId, ReceiveMode receiveMode)
	{		
		super(messagingFactory, entityPath, receiveMode);
		this.requestedSessionId = requestedSessionId;
	}	
	
	@Override
	protected final boolean isSessionReceiver()
	{
		return true;
	}
	
	@Override
	protected String getRequestedSessionId()
	{
		return this.requestedSessionId;
	}
	
	@Override
	public Instant getLockedUntilUtc() {
		return this.getInternalReceiver().getSessionLockedUntilUtc();
	}

	@Override
	public void renewLock() throws InterruptedException, ServiceBusException {
		Utils.completeFuture(this.renewLockAsync());
	}

	@Override
	public CompletableFuture<Void> renewLockAsync() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setState(InputStream stream) throws InterruptedException, ServiceBusException
	{
		Utils.completeFuture(this.setStateAsync(stream));
	}

	@Override
	public CompletableFuture<Void> setStateAsync(InputStream stream) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getState() throws InterruptedException, ServiceBusException {
		return Utils.completeFuture(this.getStateAsync());
	}

	@Override
	public CompletableFuture<InputStream> getStateAsync() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSessionId() {
		return this.getInternalReceiver().getSessionId();
	}
}
