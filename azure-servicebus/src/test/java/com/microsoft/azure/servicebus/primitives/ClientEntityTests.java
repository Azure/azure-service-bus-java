package com.microsoft.azure.servicebus.primitives;

import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

public class ClientEntityTests {
	
	@Test
	public void closeMultipleTimesTest() {
		int expectedNumberOfCloseFailures = 5;
		TestClientEntity clientEntity = new TestClientEntity(expectedNumberOfCloseFailures);
		int actualCloseFailures = 0;
		for (int i=0; i<expectedNumberOfCloseFailures + 1; i++)
		{
			try {
				clientEntity.close();
			} catch (Exception e) {
				actualCloseFailures++;
			}
		}
		
		Assert.assertTrue("Entity not closed even after retries.", clientEntity.getIsClosed());
		Assert.assertEquals("Close didn't fail expected number of times", expectedNumberOfCloseFailures, actualCloseFailures);
	}
	
	@Test
	public void closeAlreadyClosedEntityTest() {
		TestClientEntity clientEntity = new TestClientEntity(0);
		for(int i=0; i<5; i++) {
			try {
				clientEntity.close();
			} catch (ServiceBusException e) {
				Assert.fail("Entity close threw unexpected exception");
			}
		}
		
		Assert.assertTrue("Entity not closed.", clientEntity.getIsClosed());
		Assert.assertEquals("OnClose called on already closed entity.", 1, clientEntity.getTotalNumberOfCloseCalls());
	}
	
	@Test
	public void concurrentEntityCloseTest() throws InterruptedException, ExecutionException {
		int numConcurrentCalls = 10;
		Duration sleepInCloseDuration = Duration.ofSeconds(5);
		TestClientEntity clientEntity = new TestClientEntity(0, sleepInCloseDuration);
		ArrayList<ForkJoinTask<CompletableFuture<Void>>> tasks = new ArrayList<>();
		
		for (int i=0; i<numConcurrentCalls + 1; i++)
		{
			tasks.add(ForkJoinPool.commonPool().submit(() -> clientEntity.closeAsync()));
		}
		
		for (ForkJoinTask<CompletableFuture<Void>> task : tasks) {
			Assert.assertFalse("Entity closed early.", task.get().isDone());
		}
		
		Assert.assertTrue("Entity not closing even after calling close.", clientEntity.getIsClosingOrClosed());
		Assert.assertFalse("Entity closed without sleeping.", clientEntity.getIsClosed());
		
		Thread.sleep(sleepInCloseDuration.toMillis() + 500); // 500 millis buffer
		
		for (ForkJoinTask<CompletableFuture<Void>> task : tasks) {
			Assert.assertTrue("Entity not closed even after delay.", task.get().isDone());
		}
		
		Assert.assertTrue("Entity not closed even after calling close.", clientEntity.getIsClosed());
		Assert.assertEquals("OnClose called more than expected number of times", 1, clientEntity.getTotalNumberOfCloseCalls());
	}
	
	@Test
	public void concurrentEntityCloseFailureTest() throws InterruptedException, ExecutionException {
		int numConcurrentCalls = 10;
		Duration sleepInCloseDuration = Duration.ofSeconds(5);
		TestClientEntity clientEntity = new TestClientEntity(1, sleepInCloseDuration, true);
		ArrayList<ForkJoinTask<CompletableFuture<Void>>> tasks = new ArrayList<>();
		
		for (int i=0; i<numConcurrentCalls + 1; i++)
		{
			tasks.add(ForkJoinPool.commonPool().submit(() -> clientEntity.closeAsync()));
		}
		
		for (ForkJoinTask<CompletableFuture<Void>> task : tasks) {
			Assert.assertFalse("Entity close failed too early.", task.get().isDone());
		}
		
		Assert.assertTrue("Entity not closing even after calling close.", clientEntity.getIsClosingOrClosed());
		Assert.assertFalse("Entity closed without sleeping.", clientEntity.getIsClosed());
		
		Thread.sleep(sleepInCloseDuration.toMillis() + 500); // 500 millis buffer
		
		Throwable failureException = null;
		for (ForkJoinTask<CompletableFuture<Void>> task : tasks) {
			CompletableFuture<Void> closeFuture = task.get();
			Assert.assertTrue("Entity close didn't fail even after delay.", closeFuture.isDone());
			try {
				closeFuture.get();
				Assert.fail("Entity close didn't fail.");
			}catch (ExecutionException ex) {
				Throwable cause = ex.getCause();
				if (failureException == null) {
					failureException = cause;
				}
				else
				{
					Assert.assertEquals("All concurrent close failures didn't fail with the same exception.", failureException, cause);
				}
			}
		}
	}
	
	class TestClientEntity extends ClientEntity {

		private final Duration sleepDurationInClose;
		private final int targetNumberOfCloseFailures;
		private final boolean shouldSleepInFailure;
		private AtomicInteger numberOfCloseFailures = new AtomicInteger(0);		
		private AtomicInteger numberOfCloseCalls = new AtomicInteger(0);
		
		TestClientEntity(int targetNumberOfCloseFailures) {
			this(targetNumberOfCloseFailures, Duration.ZERO);
		}
		
		TestClientEntity(int targetNumberOfCloseFailures, Duration sleepDurationInClose) {
			this(targetNumberOfCloseFailures, sleepDurationInClose, false);
		}
		
		TestClientEntity(int targetNumberOfCloseFailures, Duration sleepDurationInClose, boolean shouldSleepInFailure) {
			super(UUID.randomUUID().toString());
			this.sleepDurationInClose = sleepDurationInClose;
			this.targetNumberOfCloseFailures = targetNumberOfCloseFailures;
			this.shouldSleepInFailure = shouldSleepInFailure;
		}
		
		@Override
		protected CompletableFuture<Void> onClose() {
			numberOfCloseCalls.incrementAndGet();
			if(numberOfCloseFailures.get() < targetNumberOfCloseFailures)
			{
				numberOfCloseFailures.incrementAndGet();
				CompletableFuture<Void> failFuture = new CompletableFuture<>();
				if (this.shouldSleepInFailure) {
					if (this.sleepDurationInClose.isZero()) {
						failFuture.completeExceptionally(new Exception("Close failed."));
					} else {
						Thread completionThread = new Thread(() -> {
							try {
								Thread.sleep(this.sleepDurationInClose.toMillis());
							} catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							}
							
							failFuture.completeExceptionally(new Exception("Close failed."));
						});
						
						completionThread.start();
					}
				} else {
					failFuture.completeExceptionally(new Exception("Close failed."));
				}
				
				return failFuture;
			}
			else
			{
				if (this.sleepDurationInClose.isZero()) {
					return CompletableFuture.completedFuture(null);
				} else {
					CompletableFuture<Void> closeFuture = new CompletableFuture<>();
					
					Thread completionThread = new Thread(() -> {
						try {
							Thread.sleep(this.sleepDurationInClose.toMillis());
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						
						closeFuture.complete(null);
					});
					
					completionThread.start();
					return closeFuture;
				}
			}
		}
		
		int getTotalNumberOfCloseCalls() {
			return this.numberOfCloseCalls.get();
		}
	}
}
