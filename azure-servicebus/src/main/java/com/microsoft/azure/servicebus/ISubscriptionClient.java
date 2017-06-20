package com.microsoft.azure.servicebus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.rules.Filter;
import com.microsoft.azure.servicebus.rules.RuleDescription;

public interface ISubscriptionClient extends IMessageEntity, IMessageAndSessionPump
{
	public ReceiveMode getReceiveMode();
	public void addRule(RuleDescription ruleDescription) throws InterruptedException, ServiceBusException;
	public CompletableFuture<Void> addRuleAsync(RuleDescription ruleDescription);
	public void addRule(String ruleName, Filter filter) throws InterruptedException, ServiceBusException;
	public CompletableFuture<Void> addRuleAsync(String ruleName, Filter filter);
	public CompletableFuture<Void> removeRuleAsync(String ruleName);
	public void removeRule(String ruleName) throws InterruptedException, ServiceBusException;
	public List<RuleDescription> getRules() throws ServiceBusException, InterruptedException;
	public CompletableFuture<List<RuleDescription>> getRulesAsync();
	public String getTopicName();
	public String getSubscriptionName();
}
