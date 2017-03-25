package com.microsoft.azure.servicebus;

import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.MessagingEntityAlreadyExistsException;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.rules.CorrelationFilter;
import com.microsoft.azure.servicebus.rules.RuleDescription;
import com.microsoft.azure.servicebus.rules.SqlFilter;
import com.microsoft.azure.servicebus.rules.SqlRuleAction;
import com.microsoft.azure.servicebus.rules.TrueFilter;

public class SubscriptionClientTests {
	private TopicClient topicClient;
	private TopicClient sessionfulTopicClient;
	private SubscriptionClient subscriptionClient;
	private SubscriptionClient sessionfulSubscriptionClient;
	
	@Before
	public void setup()
	{		
		
	}	
	
	@After
	public void tearDown() throws ServiceBusException, InterruptedException
	{
		if(this.topicClient != null)
		{
			this.topicClient.close();
		}
		if(this.sessionfulTopicClient != null)
		{
			this.sessionfulTopicClient.close();
		}
		if(this.subscriptionClient != null)
		{
			this.subscriptionClient.close();
			TestCommons.drainAllMessages(TestUtils.getSubscriptionConnectionStringBuilder());
		}
			
		if(this.sessionfulSubscriptionClient != null)
		{
			this.sessionfulSubscriptionClient.close();
			TestCommons.drainAllMessages(TestUtils.getSessionfulSubscriptionConnectionStringBuilder());
		}			
	}	
	
	private void createSubscriptionClient() throws InterruptedException, ServiceBusException
	{
		this.topicClient = new TopicClient(TestUtils.getTopicConnectionStringBuilder().toString());
		this.subscriptionClient = new SubscriptionClient(TestUtils.getSubscriptionConnectionStringBuilder().toString(), ReceiveMode.PeekLock);
	}
	
	private void createSessionfulSubscriptionClient() throws InterruptedException, ServiceBusException
	{
		this.sessionfulTopicClient = new TopicClient(TestUtils.getSessionfulTopicConnectionStringBuilder().toString());
		this.sessionfulSubscriptionClient = new SubscriptionClient(TestUtils.getSessionfulSubscriptionConnectionStringBuilder().toString(), ReceiveMode.PeekLock);
	}
	
	@Test
	public void testAddRemoveRules() throws InterruptedException, ServiceBusException
	{
		this.createSubscriptionClient();
		
		// Simple rule
		RuleDescription trueFilterRule = new RuleDescription("customRule1", TrueFilter.DEFAULT);
		this.subscriptionClient.addRule(trueFilterRule);
		try
		{
			this.subscriptionClient.addRule(trueFilterRule);
			Assert.fail("A rule with duplicate name is added.");
		}
		catch(MessagingEntityAlreadyExistsException e)
		{
			// Expected
		}		
		this.subscriptionClient.removeRule(trueFilterRule.getName());
		
		// Custom SQL Filter rule
		SqlFilter sqlFilter = new SqlFilter("1=1");
		SqlRuleAction action = new SqlRuleAction("set FilterTag = 'true'");
		RuleDescription sqlRule = new RuleDescription("customRule2", sqlFilter);
		sqlRule.setAction(action);
		this.subscriptionClient.addRule(sqlRule);		
		this.subscriptionClient.removeRule(sqlRule.getName());
		
		// Correlation Filter rule
		CorrelationFilter correlationFilter = new CorrelationFilter();
		correlationFilter.setCorrelationId("TestCorrelationId");
		correlationFilter.setMessageId("TestMessageId");
		correlationFilter.setReplyTo("testReplyTo");
		correlationFilter.setLabel("testLabel");
		correlationFilter.setTo("testTo");
		correlationFilter.setReplyTo("testReplyTo");
		HashMap<String, Object> properties = new HashMap<>();
		properties.put("testKey1", "testValue1");
		properties.put("testKey2", null);
		correlationFilter.setProperties(properties);		
		RuleDescription correlationRule = new RuleDescription("customRule3", correlationFilter);
		correlationRule.setAction(action);
		this.subscriptionClient.addRule(correlationRule);		
		this.subscriptionClient.removeRule(correlationRule.getName());
	}
	
	@Test
	public void testMessagePumpAutoComplete() throws InterruptedException, ServiceBusException
	{
		this.createSubscriptionClient();
		MessagePumpTests.testMessagePumpAutoComplete(this.topicClient, this.subscriptionClient);
	}
	
	@Test
	public void testMessagePumpClientComplete() throws InterruptedException, ServiceBusException
	{
		this.createSubscriptionClient();
		MessagePumpTests.testMessagePumpClientComplete(this.topicClient, this.subscriptionClient);
	}
	
	@Test
	public void testMessagePumpAbandonOnException() throws InterruptedException, ServiceBusException
	{
		this.createSubscriptionClient();
		MessagePumpTests.testMessagePumpAbandonOnException(this.topicClient, this.subscriptionClient);
	}
	
	@Test
	public void testMessagePumpRenewLock() throws InterruptedException, ServiceBusException
	{
		this.createSubscriptionClient();
		MessagePumpTests.testMessagePumpRenewLock(this.topicClient, this.subscriptionClient);
	}
}
