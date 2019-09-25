package com.microsoft.azure.servicebus.jms;

public class ServiceBusJmsConnectionFactorySettings {
    private long connectionIdleTimeoutMS;
    private boolean traceFrames;
    
    public ServiceBusJmsConnectionFactorySettings() { }
    
    public ServiceBusJmsConnectionFactorySettings(long connectionIdleTimeoutMS, boolean traceFrames) {
        this.connectionIdleTimeoutMS = connectionIdleTimeoutMS;
        this.traceFrames = traceFrames;
    }
    
    public long getConnectionIdleTimeoutMS() {
        return connectionIdleTimeoutMS;
    }
    
    public void setConnectionIdleTimeoutMS(long connectionIdleTimeoutMS) {
        this.connectionIdleTimeoutMS = connectionIdleTimeoutMS;
    }
    
    public boolean isTraceFrames() {
        return traceFrames;
    }
    
    public void setTraceFrames(boolean traceFrames) {
        this.traceFrames = traceFrames;
    }
    
    String toQuery() {
        StringBuilder builder = new StringBuilder();
        if (connectionIdleTimeoutMS > 0) {
            appendQuery(builder, "amqp.idleTimeout", String.valueOf(connectionIdleTimeoutMS));
        }
        if (traceFrames) {
            appendQuery(builder, "amqp.traceFrames", "true");
        }
        return builder.toString();
    }
    
    private static void appendQuery(StringBuilder builder, String key, String val) {
        builder.append(builder.length() == 0 ? "?" : "&").append(key).append("=").append(val);
    }
}
