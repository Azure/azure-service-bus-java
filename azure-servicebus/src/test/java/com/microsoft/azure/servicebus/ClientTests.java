package com.microsoft.azure.servicebus;

import java.net.URI;
import java.util.concurrent.ExecutionException;

import com.microsoft.azure.servicebus.management.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public abstract class ClientTests extends Tests{
    private static String entityNameCreatedForAllTests = null;
    private static String receiveEntityPathForAllTest = null;
    
    private String entityName;
    private String receiveEntityPath;
    protected IMessageSender sendClient;
    protected IMessageAndSessionPump receiveClient;
    protected ManagementClient managementClient;
    
    @BeforeClass
    public static void init()
    {
        ClientTests.entityNameCreatedForAllTests = null;
        ClientTests.receiveEntityPathForAllTest = null;
    }
    
    @Before
    public void setup() throws InterruptedException, ExecutionException, ServiceBusException, ManagementException
    {
        URI namespaceEndpointURI = TestUtils.getNamespaceEndpointURI();
        ClientSettings managementClientSettings = TestUtils.getManagementClientSettings();
        this.managementClient = new ManagementClient(namespaceEndpointURI, managementClientSettings);

        if(this.shouldCreateEntityForEveryTest() || ClientTests.entityNameCreatedForAllTests == null)
        {
             // Create entity
            //this.entityName = TestUtils.randomizeEntityName(this.getEntityNamePrefix());
            this.entityName = "queue1";
            if(this.isEntityQueue())
            {
                this.receiveEntityPath = this.entityName;
                QueueDescription queueDescription = new QueueDescription(this.entityName);
                queueDescription.setEnablePartitioning(this.isEntityPartitioned());
                this.managementClient.createQueueAsync(queueDescription);
                if(!this.shouldCreateEntityForEveryTest())
                {
                    ClientTests.entityNameCreatedForAllTests = entityName;
                    ClientTests.receiveEntityPathForAllTest = entityName;
                }
            }
            else
            {
                // todo
                /*TopicDescription topicDescription = new TopicDescription(this.entityName);
                topicDescription.setEnablePartitioning(this.isEntityPartitioned());
                ManagementClient.createEntity(namespaceEndpointURI, managementClientSettings, topicDescription);
                SubscriptionDescription subDescription = new SubscriptionDescription(this.entityName, TestUtils.FIRST_SUBSCRIPTION_NAME);
                ManagementClient.createEntity(namespaceEndpointURI, managementClientSettings, subDescription);
                this.receiveEntityPath = subDescription.getPath();
                if(!this.shouldCreateEntityForEveryTest())
                {
                    ClientTests.entityNameCreatedForAllTests = entityName;
                    ClientTests.receiveEntityPathForAllTest = subDescription.getPath();
                }*/
            }
        }
        else
        {
            this.entityName = ClientTests.entityNameCreatedForAllTests;
            this.receiveEntityPath = ClientTests.receiveEntityPathForAllTest;
        }
    }
    
    @After
    public void tearDown() throws ServiceBusException, InterruptedException, ExecutionException, ManagementException
    {
        if(this.sendClient != null)
        {
            this.sendClient.close();
        }
        if(this.receiveClient != null)
        {
            if(this.receiveClient instanceof SubscriptionClient)
            {
                ((SubscriptionClient)this.receiveClient).close();
            }
            else
            {
                ((QueueClient)this.receiveClient).close();
            }
        }
        
        if(this.shouldCreateEntityForEveryTest())
        {
            // todo
            //ManagementClient.deleteEntity(TestUtils.getNamespaceEndpointURI(), TestUtils.getManagementClientSettings(), this.entityName);
        }
        else
        {
            TestCommons.drainAllMessages(this.receiveEntityPath);
        }
    }
    
    @AfterClass
    public static void cleanupAfterAllTest() throws ManagementException
    {
        if(ClientTests.entityNameCreatedForAllTests != null)
        {
            // todo
            //ManagementClient.deleteEntity(TestUtils.getNamespaceEndpointURI(), TestUtils.getManagementClientSettings(), ClientTests.entityNameCreatedForAllTests);
        }
    }
    
    protected void createClients(ReceiveMode receiveMode) throws InterruptedException, ServiceBusException
    {
        if(this.isEntityQueue())
        {
            this.sendClient = new QueueClient(TestUtils.getNamespaceEndpointURI(), this.entityName, TestUtils.getClientSettings(), receiveMode);
            this.receiveClient = (QueueClient)this.sendClient;
        }
        else
        {
            this.sendClient = new TopicClient(TestUtils.getNamespaceEndpointURI(), this.entityName, TestUtils.getClientSettings());
            this.receiveClient = new SubscriptionClient(TestUtils.getNamespaceEndpointURI(), this.receiveEntityPath, TestUtils.getClientSettings(), receiveMode);
        }
    }
    
    @Test
    public void testMessagePumpAutoComplete() throws InterruptedException, ServiceBusException
    {
        this.createClients(ReceiveMode.PEEKLOCK);
        MessageAndSessionPumpTests.testMessagePumpAutoComplete(this.sendClient, this.receiveClient);
    }
    
    @Test
    public void testReceiveAndDeleteMessagePump() throws InterruptedException, ServiceBusException
    {
        this.createClients(ReceiveMode.RECEIVEANDDELETE);
        MessageAndSessionPumpTests.testMessagePumpAutoComplete(this.sendClient, this.receiveClient);
    }
    
    @Test
    public void testMessagePumpClientComplete() throws InterruptedException, ServiceBusException
    {
        this.createClients(ReceiveMode.PEEKLOCK);
        MessageAndSessionPumpTests.testMessagePumpClientComplete(this.sendClient, this.receiveClient);
    }
    
    @Test
    public void testMessagePumpAbandonOnException() throws InterruptedException, ServiceBusException
    {
        this.createClients(ReceiveMode.PEEKLOCK);
        MessageAndSessionPumpTests.testMessagePumpAbandonOnException(this.sendClient, this.receiveClient);
    }
    
    @Test
    public void testMessagePumpRenewLock() throws InterruptedException, ServiceBusException
    {
        this.createClients(ReceiveMode.PEEKLOCK);
        MessageAndSessionPumpTests.testMessagePumpRenewLock(this.sendClient, this.receiveClient);
    }
    
    @Test
    public void testRegisterAnotherHandlerAfterMessageHandler() throws InterruptedException, ServiceBusException
    {
        this.createClients(ReceiveMode.PEEKLOCK);
        MessageAndSessionPumpTests.testRegisterAnotherHandlerAfterMessageHandler(this.receiveClient);
    }
}
