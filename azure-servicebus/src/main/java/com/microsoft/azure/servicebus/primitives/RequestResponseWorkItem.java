package com.microsoft.azure.servicebus.primitives;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.proton.message.Message;

public class RequestResponseWorkItem extends WorkItem<Message>
{
	Message request;
	ByteBuffer txnId;
	
	public RequestResponseWorkItem(Message request, ByteBuffer txnId, CompletableFuture<Message> completableFuture, TimeoutTracker tracker) {
		super(completableFuture, tracker);
		this.request = request;
		this.txnId = txnId;
	}
	
	public RequestResponseWorkItem(Message request, ByteBuffer txnId, CompletableFuture<Message> completableFuture, Duration timeout) {
		super(completableFuture, timeout);
		this.request = request;
		this.txnId = txnId;
	}
	
	public Message getRequest()
	{
		return this.request;
	}

	public ByteBuffer getTxnId() { return this.txnId; }
}
