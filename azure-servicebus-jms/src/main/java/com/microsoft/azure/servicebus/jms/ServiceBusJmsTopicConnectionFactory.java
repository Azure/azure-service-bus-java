package com.microsoft.azure.servicebus.jms;

import javax.jms.JMSException;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;

public class ServiceBusJmsTopicConnectionFactory extends ServiceBusJmsConnectionFactory implements TopicConnectionFactory {

    public ServiceBusJmsTopicConnectionFactory(String connectionString, ServiceBusJmsConnectionFactorySettings settings) {
        super(connectionString, settings);
    }
    
    public ServiceBusJmsTopicConnectionFactory(ConnectionStringBuilder connectionStringBuilder, ServiceBusJmsConnectionFactorySettings settings) {
        super(connectionStringBuilder, settings);
    }
    
    public ServiceBusJmsTopicConnectionFactory(String sasKeyName, String sasKey, String host, ServiceBusJmsConnectionFactorySettings settings) {
        super(sasKeyName, sasKey, host, settings);
    }

    public TopicConnection createTopicConnection() throws JMSException {
        return this.getConectionFactory().createTopicConnection();
    }

    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return this.getConectionFactory().createTopicConnection(userName, password);
    }

}
