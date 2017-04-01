package com.microsoft.azure.servicebus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.MiscRequestResponseOperationHandler;
import com.microsoft.azure.servicebus.primitives.Pair;

final class SessionBrowser {
	private static final int PAGESIZE = 100;
	// .net DateTime.MaxValue need to be passed
	private static final Date MAXDATE = new Date(253402300800000l);	
	
	private final MessagingFactory messagingFactory;
	private final String entityPath;
	private MiscRequestResponseOperationHandler miscRequestResponseHandler;	
	
	SessionBrowser(MessagingFactory messagingFactory, String entityPath, MiscRequestResponseOperationHandler miscRequestResponseHandler)
	{		
		this.messagingFactory = messagingFactory;		
		this.entityPath = entityPath;
		this.miscRequestResponseHandler = miscRequestResponseHandler;
	}
	
	public CompletableFuture<Collection<IMessageSession>> getMessageSessionsAsync()
	{
		return this.getMessageSessionsAsync(MAXDATE);
	}
	
	public CompletableFuture<Collection<IMessageSession>> getMessageSessionsAsync(Date lastUpdatedTime)
	{
		return this.getMessageSessionsAsync(lastUpdatedTime, 0, null);
	}
	
	private CompletableFuture<Collection<IMessageSession>> getMessageSessionsAsync(Date lastUpdatedTime, int lastReceivedSkip, String lastSessionId)
	{
		return this.miscRequestResponseHandler.getMessageSessionsAsync(lastUpdatedTime, lastReceivedSkip, PAGESIZE, lastSessionId).thenComposeAsync((p) ->
		{						
			int newLastReceivedSkip = p.getSecondItem();
			String[] sessionIds = p.getFirstItem();
			ArrayList<IMessageSession> sessionsList = new ArrayList<>();			
			if(sessionIds != null && sessionIds.length > 0)
			{
				CompletableFuture[] initFutures = new CompletableFuture[sessionIds.length];
				int initFutureIndex = 0;
				String newLastSessionId = sessionIds[sessionIds.length - 1];
				for(String sessionId : sessionIds)
				{
					BrowsableMessageSession browsableSession = new BrowsableMessageSession(sessionId, this.messagingFactory, this.entityPath);
					sessionsList.add(browsableSession);
					initFutures[initFutureIndex++] = browsableSession.initializeAsync();					
				}
				CompletableFuture<Void> allInitFuture = CompletableFuture.allOf(initFutures);
				return allInitFuture.thenComposeAsync((v) -> getMessageSessionsAsync(lastUpdatedTime, newLastReceivedSkip, newLastSessionId)).thenApply((c) -> {
					sessionsList.addAll(c);
					return sessionsList;
				});
			}
			else
			{
				return CompletableFuture.completedFuture(sessionsList);
			}			
		});
	}
}
