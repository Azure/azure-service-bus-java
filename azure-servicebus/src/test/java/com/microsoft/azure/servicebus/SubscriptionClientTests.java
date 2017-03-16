package com.microsoft.azure.servicebus;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

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
	private ConnectionStringBuilder subscriptionBuilder;	
	private SubscriptionClient subscriptionClient;
	
	@Before
	public void setup() throws IOException, InterruptedException, ExecutionException, ServiceBusException
	{
		this.subscriptionBuilder = TestUtils.getSubscriptionConnectionStringBuilder();
		this.subscriptionClient = new SubscriptionClient(this.subscriptionBuilder.toString(), ReceiveMode.PeekLock);
	}
	
	@After
	public void tearDown() throws ServiceBusException, IOException, InterruptedException, ExecutionException
	{
		this.subscriptionClient.close();
	}
	
	@Test
	public void testAddRemoveRules() throws InterruptedException, ServiceBusException
	{		
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
}
