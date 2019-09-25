package com.microsoft.azure.servicebus.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.MessageProducer;
import javax.jms.Topic;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.azure.servicebus.management.ManagementClientAsync;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;

public class TopicTests {
    private static final String CONNECTION_STRING = "Endpoint=sb://contoso.servicebus.onebox.windows-int.net/;SharedAccessKeyName=DefaultNamespaceSasAllKeyName;SharedAccessKey=8864/auVd3qDC75iTjBL1GJ4D2oXC6bIttRd0jzDZ+g=";
    private static ConnectionFactory CONNECTION_FACTORY;
    private static ManagementClientAsync managementClient;
    private Connection connection = null;
    private Session session = null;
    private MessageProducer producer = null;
    private Topic topic = null;
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
        topic = session.createTopic(UUID.randomUUID().toString());
        entityName = topic.getTopicName();
    }
    
    @After
    public void testCleanup() throws JMSException {
        if (producer != null) producer.close();
        if (session != null) session.close();
        if (connection != null) connection.close();
        if (managementClient.topicExistsAsync(entityName).join()) managementClient.deleteTopicAsync(entityName).join();
    }
    
    @AfterClass
    public static void suiteCleanup() throws IOException {
        managementClient.close();
    }
    
    // Creating producer will send AMQP ATTACH frame to broker
    // If the topic entity does not exist yet, it will be created before link is created
    @Test
    public void createTopicOnAttach() throws JMSException {
        assertFalse("Entity should not exit before the test. Entity name: " + entityName, managementClient.topicExistsAsync(entityName).join());
        System.out.println("Attaching producer to topic when it does not exist yet.");
        producer = session.createProducer(topic);
        assertTrue("Entity should now exit after the AMQP Attach. Entity name: " + entityName, managementClient.topicExistsAsync(entityName).join());
        producer.close();
        
        assertTrue("Entity should still exit after the AMQP Attach. Entity name: " + entityName, managementClient.topicExistsAsync(entityName).join());
        System.out.println("Attaching producer to topic after it's created.");
        producer = session.createProducer(topic);
        System.out.println("Producer attached.");
    }
    
    @Test
    public void sendTest() throws JMSException {
        producer = session.createProducer(topic);
        System.out.println("Sending link created.");
        
        String requestText = "This is the request message.";
        TextMessage message = session.createTextMessage(requestText);
        System.out.println("Sending message...");
        producer.send(message);
        System.out.println("Message sent.");
    }
    
    // Creating a topic through AMQP Attach should still require the proper authorization from SAS
    @Test
    public void createTopicWithoutAuthorization() throws JMSException {
        System.out.println("Produce a connection string with a bad SasKey:");
        String invalidSasConnectionString = CONNECTION_STRING.valueOf(CONNECTION_STRING).replaceAll("(SharedAccessKey=)(.*)(=)", "SharedAccessKey=BadSasKey");
        System.out.println(invalidSasConnectionString);
        
        ConnectionStringBuilder builder = new ConnectionStringBuilder(invalidSasConnectionString);
        connection = CONNECTION_FACTORY.createConnection(builder.getSasKeyName(), builder.getSasKey());
        connection.start();
        session = connection.createSession(false, 1);
        topic = session.createTopic(entityName);
        
        System.out.println("Create a new topic through AMQP Attach.");
        try {
            producer = session.createProducer(topic);
            fail("Should have failed trying to create the topic with bad SasKey.");
        } catch (Exception e) {
            System.out.println("JMSSecurityException should be thrown, caught exception: " + e.getClass().toString());
            assertEquals(JMSSecurityException.class, e.getClass());
        } finally {
            assertFalse("Entity should not have been created. Entity name: " + entityName, managementClient.topicExistsAsync(entityName).join());
        }
    }
}
