package com.microsoft.azure.servicebus.management;

import java.time.Duration;

public class SubscriptionDescription {
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

    public void setRequiresSession(boolean requiresSession) {
        this.requiresSession = requiresSession;
    }
    
    public String getEntityPath()
    {
        return this.topicPath + "/subscriptions/" + this.name;
    }
}
