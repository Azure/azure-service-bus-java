package com.microsoft.azure.servicebus.management;

import java.time.Duration;

public class SubscriptionDescription extends ResourceDescripton {
    private static final String ATOM_XML_FORMAT = "<entry xmlns=\"http://www.w3.org/2005/Atom\">"
            + "<content type=\"application/xml\">"
                 + "<SubscriptionDescription xmlns=\"http://schemas.microsoft.com/netservices/2010/10/servicebus/connect\" xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">"
                      + "<LockDuration>%s</LockDuration>"
                      + "<RequiresSession>%b</RequiresSession>"
                      + "<DefaultMessageTimeToLive>%s</DefaultMessageTimeToLive>"
                      + "<MaxDeliveryCount>%s</MaxDeliveryCount>"
                 + "</SubscriptionDescription>"
            + "</content>"
          + "</entry>";
    
    private String topicPath;
    private String name;
    private String forwardTo;
    private String forwardDeadLetteredMessagesTo;
    private int maxDeliveryCount;
    private boolean requiresSession;
    private boolean enableDeadLetteringOnMessageExpiration;
    private boolean enableDeadLetteringOnFilterEvaluationExceptions;
    private Duration lockDuration;
    private Duration defaultMessageTimeToLive;
    private Duration autoDeleteOnIdle;
    
    public SubscriptionDescription(String topicPath, String subscriptionName)
    {
        this.topicPath = topicPath;
        this.name = subscriptionName;
        this.defaultMessageTimeToLive = Duration.ofDays(7);
        this.lockDuration = Duration.ofMinutes(1);
        this.maxDeliveryCount = 10;
    }

    public String getTopicPath() {
        return topicPath;
    }

    public void setTopicPath(String topicPath) {
        this.topicPath = topicPath;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isRequiresSession() {
        return requiresSession;
    }
    
    public Duration getLockDuration() {
        return lockDuration;
    }

    public void setLockDuration(Duration lockDuration) {
        this.lockDuration = lockDuration;
    }

    public void setRequiresSession(boolean requiresSession) {
        this.requiresSession = requiresSession;
    }
    
    @Override
    public String getPath()
    {
        return this.topicPath + "/subscriptions/" + this.name;
    }    
    
    public int getMaxDeliveryCount() {
        return maxDeliveryCount;
    }

    public void setMaxDeliveryCount(int maxDeliveryCount) {
        this.maxDeliveryCount = maxDeliveryCount;
    }

    @Override
    String getAtomXml()
    {
        return String.format(ATOM_XML_FORMAT, SerializerUtil.serializeDuration(this.lockDuration), this.requiresSession, SerializerUtil.serializeDuration(this.defaultMessageTimeToLive), this.maxDeliveryCount);
    }
}
