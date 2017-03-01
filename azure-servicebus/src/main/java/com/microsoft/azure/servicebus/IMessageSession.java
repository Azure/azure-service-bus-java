package com.microsoft.azure.servicebus;

import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public interface IMessageSession extends IMessageReceiver {
	String getSessionId();
	
	Instant getLockedUntilUtc();
	
	void renewLock() throws InterruptedException, ServiceBusException;
	
	CompletableFuture<Void> renewLockAsync();
	
	void setState(InputStream stream) throws InterruptedException, ServiceBusException;
	
	CompletableFuture<Void> setStateAsync(InputStream stream);
	
	InputStream getState() throws InterruptedException, ServiceBusException;
	
	CompletableFuture<InputStream> getStateAsync();	
}
