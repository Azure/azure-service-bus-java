package com.microsoft.azure.servicebus;

import java.util.concurrent.CompletableFuture;

public interface ISessionHandler
{
	public CompletableFuture<Void> onMessageAsync(IMessageSession session, IBrokeredMessage message);
	
	public CompletableFuture<Void> onSessionClosedAsync(IMessageSession session);
	
	public void notifyException(Throwable exception, ExceptionPhase phase);
}
