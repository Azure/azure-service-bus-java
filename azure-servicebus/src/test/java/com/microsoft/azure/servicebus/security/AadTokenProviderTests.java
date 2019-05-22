package com.microsoft.azure.servicebus.security;

import static org.junit.Assert.*;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.servicebus.ClientSettings;
import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.QueueClient;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.TestUtils;
import com.microsoft.azure.servicebus.TopicClient;
import com.microsoft.azure.servicebus.management.ManagementClientAsync;
import com.microsoft.azure.servicebus.management.QueueDescription;
import com.microsoft.azure.servicebus.management.TopicDescription;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.primitives.StringUtil;

public class AadTokenProviderTests {
	private static final String ENTITY_NAME_PREFIX = "TokenProviderTests";
	private static final SecurityToken TEST_TOKEN = new SecurityToken(SecurityTokenType.JWT, "testAudience", "tokenString", Instant.now(), Instant.MAX);
	
	private static ManagementClientAsync managementClient;
	private static ClientSettings settings;
	private static boolean shouldRunAuthTests = false; // To run the authentication tests please fill out fields in getClientSettings() below
	
	@BeforeClass
	public static void createEntities() 
	{
		settings = getClientSettings();
		if (settings == null)
		{
			return;
		}
		shouldRunAuthTests = true;
		ClientSettings managementClientSettings = TestUtils.getManagementClientSettings();
		managementClient = new ManagementClientAsync(TestUtils.getNamespaceEndpointURI(), managementClientSettings);
	}
	
	@Test
	public void aadCallbackTokenProviderTest() {
		TokenProvider tokenProvider = TokenProvider.createAzureActiveDirectoryTokenProvider((audience, authority) -> {
			return TEST_TOKEN;
		}, null);
		
		assertEquals(TEST_TOKEN, tokenProvider.getSecurityTokenAsync("testAudience").join());
	}
	
	@Test
    public void QueueWithAadTokenProviderTest() throws InterruptedException, ServiceBusException, ExecutionException
    {
        if (!shouldRunAuthTests)
        {
            return;
        }

		String queuePath = TestUtils.randomizeEntityName(ENTITY_NAME_PREFIX);
		QueueDescription queueDescription = new QueueDescription(queuePath);
		queueDescription.setEnablePartitioning(false);
		managementClient.createQueueAsync(queueDescription).get();
		
        ConnectionStringBuilder builder = new ConnectionStringBuilder(TestUtils.getNamespaceConnectionString());
        QueueClient queueClient = new QueueClient(builder.getEndpoint(), queuePath, settings, ReceiveMode.PEEKLOCK);

        // Send and receive messages.
        try 
        {
        	queueClient.send(new Message("aaa"));
        }
        finally
        {
        	queueClient.close();
        }
    }

	@Test
    public void TopicWithAadTokenProviderTest() throws InterruptedException, ServiceBusException, ExecutionException
    {
        if (!shouldRunAuthTests)
        {
            return;
        }
        
		String topicPath = TestUtils.randomizeEntityName(ENTITY_NAME_PREFIX);
		TopicDescription topicDescription = new TopicDescription(topicPath);
		topicDescription.setEnablePartitioning(false);
		managementClient.createTopicAsync(topicDescription).get();

        ConnectionStringBuilder builder = new ConnectionStringBuilder(TestUtils.getNamespaceConnectionString());
        TopicClient topicClient = new TopicClient(builder.getEndpoint(), topicPath, settings);

        // Send and receive messages.
        try 
        {
        	topicClient.send(new Message("aaa"));
        }
        finally
        {
        	topicClient.close();
        }
    }
    
    private static ClientSettings getClientSettings()
    {
        // Please fill out values below manually if the AAD tests should be run
        String tenantId = "";
        String aadAppId = "";
        String aadAppSecret = "";

        if (StringUtil.isNullOrEmpty(tenantId))
        {
            return null;
        }

        BiFunction<String, String, SecurityToken> authCallback = (audience, authority) ->
        {
            AuthenticationContext authContext;
			try {
				authContext = new AuthenticationContext(authority, false, ForkJoinPool.commonPool());
	            ClientCredential cc = new ClientCredential(aadAppId, aadAppSecret);
	            AuthenticationResult authResult = authContext.acquireToken(SecurityConstants.SERVICEBUS_AAD_AUDIENCE_RESOURCE_URL, cc, null).get();
	            return new SecurityToken(SecurityTokenType.JWT, audience, authResult.getAccessToken(), Instant.now(), Instant.ofEpochMilli(authResult.getExpiresAfter()));
			} 
			catch (Exception e) 
			{
				throw new RuntimeException(e);
			}
        };
        
        return new ClientSettings(TokenProvider.createAzureActiveDirectoryTokenProvider(authCallback, "https://login.windows.net/" + tenantId));
    }
}
