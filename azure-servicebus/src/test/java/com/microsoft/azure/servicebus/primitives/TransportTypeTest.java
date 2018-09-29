package com.microsoft.azure.servicebus.primitives;

import com.microsoft.azure.servicebus.TestUtils;
import com.microsoft.azure.servicebus.amqp.ConnectionHandler;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class TransportTypeTest {

    private static MessagingFactory factory;
    private static ConnectionStringBuilder namespaceConnectionStringBuilder;

    @Test
    public void transportTypeAmqpCreatesConnectionWithPort5671() throws Exception
    {
        namespaceConnectionStringBuilder = getAmqpConnectionStringBuilder();
        this.factory = MessagingFactory.createFromNamespaceEndpointURI(
                namespaceConnectionStringBuilder.getEndpoint(),
                Util.getClientSettingsFromConnectionStringBuilder(namespaceConnectionStringBuilder));

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
        namespaceConnectionStringBuilder = getAmqpWebSocketsConnectionStringBuilder();
        this.factory = MessagingFactory.createFromNamespaceEndpointURI(
                namespaceConnectionStringBuilder.getEndpoint(),
                Util.getClientSettingsFromConnectionStringBuilder(namespaceConnectionStringBuilder));

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
        if (!TestUtils.isProxyEnabled())
        {
            // If proxy env vars aren't set, can't run this
            return;
        }

        namespaceConnectionStringBuilder = getAmqpWebSocketsConnectionStringBuilder();
        TestUtils.setDefaultProxySelector();
        this.factory = MessagingFactory.createFromNamespaceEndpointURI(
                namespaceConnectionStringBuilder.getEndpoint(),
                Util.getClientSettingsFromConnectionStringBuilder(namespaceConnectionStringBuilder));

        try {
            final Field connectionHandlerField = MessagingFactory.class.getDeclaredField("connectionHandler");
            connectionHandlerField.setAccessible(true);
            final ConnectionHandler connectionHandler = (ConnectionHandler) connectionHandlerField.get(factory);

            final Method outboundSocketPort = ConnectionHandler.class.getDeclaredMethod("getOutboundSocketPort");
            outboundSocketPort.setAccessible(true);

            final Method protocolPort = ConnectionHandler.class.getDeclaredMethod("getProtocolPort");
            protocolPort.setAccessible(true);

            Assert.assertEquals(TestUtils.getProxyPort(), outboundSocketPort.invoke(connectionHandler));
            Assert.assertEquals(443, protocolPort.invoke(connectionHandler));
        } finally {
            factory.close();
        }
    }

    private static ConnectionStringBuilder getAmqpConnectionStringBuilder()
    {
        ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(TestUtils.getNamespaceConnectionString());
        connectionStringBuilder.setTransportType(TransportType.AMQP);
        return connectionStringBuilder;
    }

    private static ConnectionStringBuilder getAmqpWebSocketsConnectionStringBuilder()
    {
        ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(TestUtils.getNamespaceConnectionString());
        connectionStringBuilder.setTransportType(TransportType.AMQP_WEB_SOCKETS);
        return connectionStringBuilder;
    }
}
