package com.microsoft.azure.servicebus.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.azure.servicebus.management.ManagementClientAsync;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;

public class QueueTests {
    private static final String CONNECTION_STRING = "Endpoint=sb://contoso.servicebus.onebox.windows-int.net/;SharedAccessKeyName=DefaultNamespaceSasAllKeyName;SharedAccessKey=8864/auVd3qDC75iTjBL1GJ4D2oXC6bIttRd0jzDZ+g=";
    private static final long DEFAULT_TIMEOUT = 3000;
    private static ConnectionFactory CONNECTION_FACTORY;
    private static ManagementClientAsync managementClient;
    private Connection connection = null;
    private Session session = null;
    private MessageConsumer consumer = null;
    private MessageProducer producer = null;
    private Queue queue = null;
    private String entityName;
    
    @BeforeClass
    public static void initConnectionFactory() {
        ConnectionStringBuilder builder = new ConnectionStringBuilder(CONNECTION_STRING);
        ServiceBusJmsConnectionFactorySettings settings = new ServiceBusJmsConnectionFactorySettings(120000, true);
        CONNECTION_FACTORY = new ServiceBusJmsConnectionFactory(builder, settings);
        managementClient = new ManagementClientAsync(builder);
    }
    
    @Before
    public void init() throws JMSException {
        connection = CONNECTION_FACTORY.createConnection();
        connection.start();
        session = connection.createSession(false, 1);
        queue = session.createQueue(UUID.randomUUID().toString());
        entityName = queue.getQueueName();
    }
    
    @After
    public void testCleanup() throws JMSException {
        if (consumer != null) consumer.close();
        if (producer != null) producer.close();
        if (session != null) session.close();
        if (connection != null) connection.close();
        if (managementClient.queueExistsAsync(entityName).join()) managementClient.deleteQueueAsync(entityName).join();
    }
    
    @AfterClass
    public static void suiteCleanup() throws IOException {
        managementClient.close();
    }
    
    // Creating consumer or producer will send AMQP ATTACH frame to broker
    // If the queue entity does not exist yet, it will be created before link is created
    @Test
    public void createQueueOnAttach() throws JMSException {
        assertFalse("Entity should not exit before the test. Entity name: " + entityName, managementClient.queueExistsAsync(entityName).join());
        System.out.println("Attaching consumer and producer to queue when it does not exist yet.");
        consumer = session.createConsumer(queue);
        producer = session.createProducer(queue);
        assertTrue("Entity should now exit after the AMQP Attach. Entity name: " + entityName, managementClient.queueExistsAsync(entityName).join());
        consumer.close();
        producer.close();
        
        assertTrue("Entity should still exit after the AMQP Attach. Entity name: " + entityName, managementClient.queueExistsAsync(entityName).join());
        System.out.println("Attaching consumer and producer to queue after it's created.");
        consumer = session.createConsumer(queue);
        producer = session.createProducer(queue);
        System.out.println("Consumer and Producer attached.");
    }
    
    @Test
    public void sendReceiveTest() throws JMSException {
        consumer = session.createConsumer(queue);
        producer = session.createProducer(queue);
        System.out.println("Sending and receiving links created.");
        
        String requestText = "This is the request message.";
        TextMessage message = session.createTextMessage(requestText);
        System.out.println("Sending message...");
        producer.send(message);
        System.out.println("Message sent.");
        System.out.println("Receiving message...");
        TextMessage receivedMessage = (TextMessage) consumer.receive(DEFAULT_TIMEOUT);
        assertEquals(message, receivedMessage);
    }
    
    // Creating a queue through AMQP Attach should still require the proper authorization from SAS
    @Test
    public void createQueueWithoutAuthorization() throws JMSException {
        System.out.println("Produce a connection string with a bad SasKey:");
        String invalidSasConnectionString = CONNECTION_STRING.valueOf(CONNECTION_STRING).replaceAll("(SharedAccessKey=)(.*)(=)", "SharedAccessKey=BadSasKey");
        System.out.println(invalidSasConnectionString);
        
        ConnectionStringBuilder builder = new ConnectionStringBuilder(invalidSasConnectionString);
        connection = CONNECTION_FACTORY.createConnection(builder.getSasKeyName(), builder.getSasKey());
        connection.start();
        session = connection.createSession(false, 1);
        queue = session.createQueue(entityName);
        
        System.out.println("Create a new queue through AMQP Attach.");
        try {
            consumer = session.createConsumer(queue);
            fail("Should have failed trying to create the queue with bad SasKey.");
        } catch (Exception e) {
            System.out.println("JMSSecurityException should be thrown, caught exception: " + e.getClass().toString());
            assertEquals(JMSSecurityException.class, e.getClass());
        } finally {
            assertFalse("Entity should not have been created. Entity name: " + entityName, managementClient.queueExistsAsync(entityName).join());
        }
    }
}
