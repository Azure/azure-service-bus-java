package com.microsoft.azure.servicebus;

import com.microsoft.azure.servicebus.management.EntityManager;
import com.microsoft.azure.servicebus.management.ManagementException;
import com.microsoft.azure.servicebus.management.QueueDescription;
import com.microsoft.azure.servicebus.management.QueueDescriptionUtil;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.junit.Before;
import org.junit.Test;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class ManagementTests {

    EntityManager managementClient;

    @Before
    public void setup() throws InterruptedException, ExecutionException, ServiceBusException, ManagementException {
        URI namespaceEndpointURI = TestUtils.getNamespaceEndpointURI();
        ClientSettings managementClientSettings = TestUtils.getManagementClientSettings();
        managementClient = new EntityManager(namespaceEndpointURI, managementClientSettings);
    }

    @Test
    public void testGetQueue() throws InterruptedException, ServiceBusException, NoSuchAlgorithmException, ExecutionException, ManagementException, IOException, KeyManagementException, KeyStoreException, URISyntaxException, TransformerException, ParserConfigurationException {
        QueueDescription queue = this.managementClient.getQueue("non-partitioned-queue");
        String serializedForm = QueueDescriptionUtil.serialize(queue);
    }
}
