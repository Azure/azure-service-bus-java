package com.microsoft.azure.servicebus.primitives;

import com.microsoft.azure.servicebus.ClientSettings;
import com.microsoft.azure.servicebus.TestUtils;
import com.microsoft.azure.servicebus.amqp.ConnectionHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.*;
import java.util.LinkedList;
import java.util.List;

public class TransportTypeTest {

    private static final String NAMESPACE_CONNECTION_STRING_ENVIRONMENT_VARIABLE_NAME = "AZURE_SERVICEBUS_JAVA_CLIENT_TEST_CONNECTION_STRING";
    private static final String PROXY_HOSTNAME_ENV_VAR = "PROXY_HOSTNAME";
    private static final String PROXY_PORT_ENV_VAR = "PROXY_PORT";
    private static MessagingFactory factory;
    private static ConnectionStringBuilder namespaceConnectionStringBuilder;

    @Test
    public void transportTypeAmqpCreatesConnectionwithPort5671() throws Exception
    {
        this.factory = MessagingFactory.createFromNamespaceEndpointURI(TestUtils.getNamespaceEndpointURI(), TestUtils.getClientSettings());
        try {
            final Field connectionHandlerField = MessagingFactory.class.getDeclaredField("connectionHandler");
            connectionHandlerField.setAccessible(true);
            final ConnectionHandler connectionHandler = (ConnectionHandler) connectionHandlerField.get(factory);

            final Method outboundSocketPort = ConnectionHandler.class.getDeclaredMethod("getOutboundSocketPort");
            outboundSocketPort.setAccessible(true);

            final Method protocolPort = ConnectionHandler.class.getDeclaredMethod("getProtocolPort");
            protocolPort.setAccessible(true);

            Assert.assertEquals(5671, outboundSocketPort.invoke(connectionHandler));
            Assert.assertEquals(5671, protocolPort.invoke(connectionHandler));
        } finally {
            factory.close();
        }
    }

    @Test
    public void transportTypeAmqpWebSocketsCreatesConnectionWithPort443() throws Exception
    {
        namespaceConnectionStringBuilder = getWebSocketConnection();
        namespaceConnectionStringBuilder.setTransportType(TransportType.AMQP_WEB_SOCKETS);
        this.factory = MessagingFactory.createFromNamespaceEndpointURI(namespaceConnectionStringBuilder.getEndpoint(), Util.getClientSettingsFromConnectionStringBuilder(namespaceConnectionStringBuilder));

        try {
             final Field connectionHandlerField = MessagingFactory.class.getDeclaredField("connectionHandler");
             connectionHandlerField.setAccessible(true);
             final ConnectionHandler connectionHandler = (ConnectionHandler) connectionHandlerField.get(factory);

             final Method outboundSocketPort = ConnectionHandler.class.getDeclaredMethod("getOutboundSocketPort");
             outboundSocketPort.setAccessible(true);

             final Method protocolPort = ConnectionHandler.class.getDeclaredMethod("getProtocolPort");
             protocolPort.setAccessible(true);

             Assert.assertEquals(443, outboundSocketPort.invoke(connectionHandler));
             Assert.assertEquals(443, protocolPort.invoke(connectionHandler));
         } finally {
             factory.close();
         }
    }

    @Test
    public void transportTypeAmqpWebSocketsWithProxyCreatesConnectionWithCorrectPorts() throws Exception
    {
        namespaceConnectionStringBuilder = getWebSocketConnection();
        namespaceConnectionStringBuilder.setTransportType(TransportType.AMQP_WEB_SOCKETS);
        this.factory = MessagingFactory.createFromNamespaceEndpointURI(namespaceConnectionStringBuilder.getEndpoint(), getProxyClientSettings());
        int proxyPort = 3128;
        try {
            final Field connectionHandlerField = MessagingFactory.class.getDeclaredField("connectionHandler");
            connectionHandlerField.setAccessible(true);
            final ConnectionHandler connectionHandler = (ConnectionHandler) connectionHandlerField.get(factory);

            final Method outboundSocketPort = ConnectionHandler.class.getDeclaredMethod("getOutboundSocketPort");
            outboundSocketPort.setAccessible(true);

            final Method protocolPort = ConnectionHandler.class.getDeclaredMethod("getProtocolPort");
            protocolPort.setAccessible(true);

            Assert.assertEquals(proxyPort, outboundSocketPort.invoke(connectionHandler));
            Assert.assertEquals(443, protocolPort.invoke(connectionHandler));
        } finally {
            factory.close();
        }
    }

    private ConnectionStringBuilder getWebSocketConnection()
    {
        String namespaceConnectionString = System.getenv(NAMESPACE_CONNECTION_STRING_ENVIRONMENT_VARIABLE_NAME);
        return new ConnectionStringBuilder(namespaceConnectionString);
    }

    private static ClientSettings getProxyClientSettings()
    {
        ClientSettings clientSettings =
                Util.getClientSettingsFromConnectionStringBuilder(namespaceConnectionStringBuilder);

        ProxySelector.setDefault(new ProxySelector() {
            @Override
            public List<Proxy> select(URI uri) {
                List<Proxy> proxies = new LinkedList<>();
                proxies.add(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(
                        System.getenv(PROXY_HOSTNAME_ENV_VAR),
                        Integer.valueOf(System.getenv(PROXY_PORT_ENV_VAR)))));
                return proxies;
            }

            @Override
            public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
                // no-op
            }
        });

        return clientSettings;
    }
}
