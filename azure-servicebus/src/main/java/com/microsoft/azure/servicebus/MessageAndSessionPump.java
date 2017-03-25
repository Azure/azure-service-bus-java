package com.microsoft.azure.servicebus;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

import com.microsoft.azure.servicebus.primitives.MessageLockLostException;
import com.microsoft.azure.servicebus.primitives.OperationCancelledException;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.primitives.StringUtil;
import com.microsoft.azure.servicebus.primitives.Timer;
import com.microsoft.azure.servicebus.primitives.TimerType;

class MessageAndSessionPump extends InitializableEntity implements IMessageAndSessionPump
{
	// Larger value means few receive calls to the internal receiver and better.
	private static final Duration MESSAGE_RECEIVE_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration MINIMUM_MESSAGE_LOCK_VALIDITY = Duration.ofSeconds(4);
	private static final Duration MAXIMUM_MESSAGE_RENEW_LOCK_BUFFER = Duration.ofSeconds(10);
	private IMessageReceiver innerReceiver;
	private boolean handlerRegistered = false;
	private IMessageHandler messageHandler;
	private ISessionHandler sessionHandler;
	private MessageHandlerOptions messageHandlerOptions;
	private SessionHandlerOptions sessionHandlerOptions;
	
	public MessageAndSessionPump(IMessageReceiver innerReceiver)
	{
		super(StringUtil.getRandomString(), null);
		this.innerReceiver = innerReceiver;
	}	

	@Override
	public void registerMessageHandler(IMessageHandler handler) {
		this.registerMessageHandler(handler, new MessageHandlerOptions());		
	}

	@Override
	public void registerMessageHandler(IMessageHandler handler, MessageHandlerOptions handlerOptions) {
		this.setHandlerRegistered();
		this.messageHandler = handler;
		this.messageHandlerOptions = handlerOptions;
		
		for(int i=0; i<handlerOptions.getMaxConcurrentCalls(); i++)
		{
			this.receiveAndPumpMessage();
		}
	}

	@Override
	public void registerSessionHandler(ISessionHandler handler) {
		this.registerSessionHandler(handler, new SessionHandlerOptions());
		
	}

	@Override
	public void registerSessionHandler(ISessionHandler handler, SessionHandlerOptions handlerOptions) {
		this.setHandlerRegistered();
		this.sessionHandler = handler;
		this.sessionHandlerOptions = handlerOptions;
	}
	
	private synchronized void setHandlerRegistered()
	{
		this.throwIfClosed(null);
		
		// Only one handler is allowed to be registered per client
		if(this.handlerRegistered)
		{
			throw new UnsupportedOperationException("MessageHandler or SessionHandler already registered.");
		}
		
		this.handlerRegistered = true;
	}

	private void receiveAndPumpMessage()
	{
		if(!this.getIsClosingOrClosed())
		{
			CompletableFuture<IBrokeredMessage> receiverFuture = MessageAndSessionPump.this.innerReceiver.receiveAsync(MessageAndSessionPump.MESSAGE_RECEIVE_TIMEOUT);
			receiverFuture.handleAsync((message, receiveEx) -> {
				if(receiveEx != null)
				{
					messageHandler.notifyException(receiveEx, ExceptionPhase.RECEIVE);
					this.receiveAndPumpMessage();
				}
				else
				{
					if(message == null)
					{
						this.receiveAndPumpMessage();
					}
					else
					{
						CompletableFuture<Void> onMessageFuture;
						try
						{
							onMessageFuture = this.messageHandler.onMessageAsync(message);
						}
						catch(Exception onMessageSyncEx)
						{
							onMessageFuture = new CompletableFuture<Void>();
							onMessageFuture.completeExceptionally(onMessageSyncEx);
						}
						
						// Start renew lock part
						final MessgeRenewLockLoop renewLockLoop;
						if(this.innerReceiver.getReceiveMode() == ReceiveMode.PeekLock)
						{
							Instant stopRenewMessageLockAt = Instant.now().plus(this.messageHandlerOptions.getMaxAutoRenewDuration());
							renewLockLoop = new MessgeRenewLockLoop(this.innerReceiver, this.messageHandler, message, stopRenewMessageLockAt);
						}
						else
						{
							renewLockLoop = null;
						}
						
						onMessageFuture.handleAsync((v, onMessageEx) -> {
							if(onMessageEx != null)
							{
								this.messageHandler.notifyException(onMessageEx, ExceptionPhase.USERCALLBACK);
							}
							if(this.innerReceiver.getReceiveMode() == ReceiveMode.PeekLock)
							{
								if(renewLockLoop != null)
								{
									renewLockLoop.cancelLoop();
								}
								CompletableFuture<Void> updateDispositionFuture;
								ExceptionPhase dispositionPhase;
								if(onMessageEx == null)
								{
									// Complete message
									dispositionPhase = ExceptionPhase.COMPLETE;
									if(this.messageHandlerOptions.isAutoComplete())
									{																				
										updateDispositionFuture = this.innerReceiver.completeAsync(message.getLockToken());
									}
									else
									{
										updateDispositionFuture = CompletableFuture.completedFuture(null);
									}									
								}
								else
								{									
									// Abandon message
									dispositionPhase = ExceptionPhase.COMPLETE;
									updateDispositionFuture = this.innerReceiver.abandonAsync(message.getLockToken());
								}
								
								updateDispositionFuture.handleAsync((u, updateDispositionEx) -> {
									if(updateDispositionEx != null)
									{
										this.messageHandler.notifyException(updateDispositionEx, dispositionPhase);
									}
									this.receiveAndPumpMessage();
									return null;
								});
							}
							else
							{
								this.receiveAndPumpMessage();
							}
							
							return null;
						});
					}
					
				}
				
				return null;
			});
		}		
	}

	@Override
	CompletableFuture<Void> initializeAsync() throws Exception {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	protected CompletableFuture<Void> onClose() {
		return CompletableFuture.completedFuture(null);
	}
	
	private static class MessgeRenewLockLoop
	{
		private IMessageReceiver innerReceiver;
		private IMessageHandler messageHandler;
		private IBrokeredMessage message;
		private Instant stopRenewalAt;
		private boolean isCancelled = false;
		private ScheduledFuture<?> timerFuture;
		
		MessgeRenewLockLoop(IMessageReceiver innerReceiver, IMessageHandler messageHandler, IBrokeredMessage message, Instant stopRenewalAt)
		{
			this.innerReceiver = innerReceiver;
			this.messageHandler = messageHandler;
			this.message = message;
			this.stopRenewalAt = stopRenewalAt;
			this.loop();
		}
		
		private void loop()
		{
			if(!this.isCancelled)
			{
				Duration renewInterval = this.getNextRenewInterval();
				if(renewInterval != null && !renewInterval.isNegative())
				{
					this.timerFuture = Timer.schedule(() -> {
						this.innerReceiver.renewMessageLockAsync(message).handleAsync((v, renewLockEx) ->
						{
							if(renewLockEx != null)
							{
								this.messageHandler.notifyException(renewLockEx, ExceptionPhase.RENEWLOCK);
								if(!(renewLockEx instanceof MessageLockLostException || renewLockEx instanceof OperationCancelledException))
								{
									this.loop();
								}
							}
							else
							{
								this.loop();
							}
							
							
							return null;
						});
					}, renewInterval, TimerType.OneTimeRun);
				}
			}
		}
		
		void cancelLoop()
		{
			this.isCancelled = true;
			if(!this.timerFuture.isDone())
			{
				this.timerFuture.cancel(true);
			}
		}
		
		private Duration getNextRenewInterval()
		{			
			if(this.message.getLockedUntilUtc().isBefore(stopRenewalAt))
			{
				Duration remainingTime = Duration.between(Instant.now(), this.message.getLockedUntilUtc());
				if(remainingTime.isNegative())
				{
					// Lock likely expired. May be there is clock skew. Assume some minimum time
					remainingTime = MessageAndSessionPump.MINIMUM_MESSAGE_LOCK_VALIDITY;
				}
				
				Duration buffer = remainingTime.dividedBy(2).compareTo(MAXIMUM_MESSAGE_RENEW_LOCK_BUFFER) > 0 ? MAXIMUM_MESSAGE_RENEW_LOCK_BUFFER : remainingTime.dividedBy(2);				
				return remainingTime.minus(buffer);
			}
			else
			{
				return null;
			}			
		}
	}

	@Override
	public void abandon(UUID lockToken) throws InterruptedException, ServiceBusException {
		this.innerReceiver.abandon(lockToken);
	}

	@Override
	public void abandon(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
		this.innerReceiver.abandon(lockToken, propertiesToModify);		
	}

	@Override
	public CompletableFuture<Void> abandonAsync(UUID lockToken) {
		return this.innerReceiver.abandonAsync(lockToken);
	}

	@Override
	public CompletableFuture<Void> abandonAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
		return this.innerReceiver.abandonAsync(lockToken, propertiesToModify);
	}

	@Override
	public void complete(UUID lockToken) throws InterruptedException, ServiceBusException {
		this.innerReceiver.complete(lockToken);
	}

	@Override
	public CompletableFuture<Void> completeAsync(UUID lockToken) {
		return this.innerReceiver.completeAsync(lockToken);
	}

	@Override
	public void defer(UUID lockToken) throws InterruptedException, ServiceBusException {
		this.innerReceiver.defer(lockToken);
	}

	@Override
	public void defer(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
		this.innerReceiver.defer(lockToken, propertiesToModify);	
	}

	@Override
	public CompletableFuture<Void> deferAsync(UUID lockToken) {
		return this.innerReceiver.abandonAsync(lockToken);
	}

	@Override
	public CompletableFuture<Void> deferAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
		return this.innerReceiver.abandonAsync(lockToken, propertiesToModify);
	}

	@Override
	public void deadLetter(UUID lockToken) throws InterruptedException, ServiceBusException {
		this.innerReceiver.deadLetter(lockToken);		
	}

	@Override
	public void deadLetter(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
		this.innerReceiver.deadLetter(lockToken, propertiesToModify);		
	}

	@Override
	public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription) throws InterruptedException, ServiceBusException {
		this.innerReceiver.deadLetter(lockToken, deadLetterReason, deadLetterErrorDescription);		
	}

	@Override
	public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
		this.innerReceiver.deadLetter(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify);		
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken) {
		return this.innerReceiver.deadLetterAsync(lockToken);
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
		return this.innerReceiver.deadLetterAsync(lockToken, propertiesToModify);
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason,	String deadLetterErrorDescription) {
		return this.innerReceiver.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription);
	}

	@Override
	public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription, Map<String, Object> propertiesToModify) {
		return this.innerReceiver.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify);
	}
}
