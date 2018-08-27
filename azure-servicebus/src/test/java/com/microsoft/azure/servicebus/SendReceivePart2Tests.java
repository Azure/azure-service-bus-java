package com.microsoft.azure.servicebus;

import com.microsoft.azure.servicebus.management.ManagementClientAsync;
import com.microsoft.azure.servicebus.management.QueueDescription;
import com.microsoft.azure.servicebus.management.SubscriptionDescription;
import com.microsoft.azure.servicebus.management.TopicDescription;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.junit.*;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

public abstract class SendReceivePart2Tests extends Tests {
    private static final String NAMESPACE_CONNECTION_STRING_ENVIRONMENT_VARIABLE_NAME = "AZURE_SERVICEBUS_JAVA_CLIENT_TEST_CONNECTION_STRING";
    static ManagementClientAsync managementClient = null;
    private static String entityNameCreatedForAllTests = null;
    private static String receiveEntityPathForAllTest = null;

    MessagingFactory factory;
    IMessageSender sender;
    IMessageReceiver receiver;
    String receiveEntityPath;

    private String entityName;
    private final String sessionId = null;

    @BeforeClass
    public static void init()
    {
        SendReceivePart2Tests.entityNameCreatedForAllTests = null;
        SendReceivePart2Tests.receiveEntityPathForAllTest = null;
        URI namespaceEndpointURI = TestUtils.getNamespaceEndpointURI();
        ClientSettings managementClientSettings = TestUtils.getManagementClientSettings();
        managementClient = new ManagementClientAsync(namespaceEndpointURI, managementClientSettings);
    }

    @Before
    public void setup() throws InterruptedException, ExecutionException, ServiceBusException
    {
        if(this.shouldCreateEntityForEveryTest() || SendReceivePart2Tests.entityNameCreatedForAllTests == null)
        {
            // Create entity
            this.entityName = TestUtils.randomizeEntityName(this.getEntityNamePrefix());
            if(this.isEntityQueue())
            {
                this.receiveEntityPath = this.entityName;
                QueueDescription queueDescription = new QueueDescription(this.entityName);
                queueDescription.setEnablePartitioning(this.isEntityPartitioned());
                managementClient.createQueueAsync(queueDescription).get();
                if(!this.shouldCreateEntityForEveryTest())
                {
                    SendReceivePart2Tests.entityNameCreatedForAllTests = entityName;
                    SendReceivePart2Tests.receiveEntityPathForAllTest = entityName;
                }
            }
            else
            {
                TopicDescription topicDescription = new TopicDescription(this.entityName);
                topicDescription.setEnablePartitioning(this.isEntityPartitioned());
                managementClient.createTopicAsync(topicDescription).get();
                SubscriptionDescription subDescription = new SubscriptionDescription(this.entityName, TestUtils.FIRST_SUBSCRIPTION_NAME);
                managementClient.createSubscriptionAsync(subDescription).get();
                this.receiveEntityPath = subDescription.getPath();
                if(!this.shouldCreateEntityForEveryTest())
                {
                    SendReceivePart2Tests.entityNameCreatedForAllTests = entityName;
                    SendReceivePart2Tests.receiveEntityPathForAllTest = subDescription.getPath();
                }
            }
        }
        else
        {
            this.entityName = SendReceivePart2Tests.entityNameCreatedForAllTests;
            this.receiveEntityPath = SendReceivePart2Tests.receiveEntityPathForAllTest;
        }

        this.factory = MessagingFactory.createFromNamespaceEndpointURI(TestUtils.getNamespaceEndpointURI(), TestUtils.getClientSettings());
        this.sender = ClientFactory.createMessageSenderFromEntityPath(this.factory, this.entityName);
    }

    @After
    public void tearDown() throws ServiceBusException, InterruptedException, ExecutionException
    {
        if(!this.shouldCreateEntityForEveryTest())
        {
            this.drainAllMessages();
        }

        this.sender.close();
        if(this.receiver != null)
            this.receiver.close();
        this.factory.close();

        if(this.shouldCreateEntityForEveryTest())
        {
            managementClient.deleteQueueAsync(this.entityName).get();
        }
    }

    @AfterClass
    public static void cleanupAfterAllTest() throws ExecutionException, InterruptedException, IOException {
        if(SendReceivePart2Tests.entityNameCreatedForAllTests != null)
        {
            managementClient.deleteQueueAsync(SendReceivePart2Tests.entityNameCreatedForAllTests).get();
            managementClient.close();
        }
    }

    @Test
    public void testSendReceiveMessageWithVariousPropertyTypes() throws InterruptedException, ServiceBusException
    {
        this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.receiveEntityPath, ReceiveMode.RECEIVEANDDELETE);
        TestCommons.testSendReceiveMessageWithVariousPropertyTypes(this.sender, this.sessionId, this.receiver);
    }

    private void drainAllMessages() throws InterruptedException, ServiceBusException
    {
        if(this.receiver != null)
        {
            TestCommons.drainAllMessagesFromReceiver(this.receiver);
        }
    }
}
