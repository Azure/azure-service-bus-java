package com.microsoft.azure.servicebus.management;

import java.time.Instant;

/**
 * This provides runtime information of the subscription.
 */
public class SubscriptionRuntimeInfo {
    private String topicPath;
    private String subscriptionName;
    private long messageCount;
    private MessageCountDetails messageCountDetails;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant accessedAt;

    SubscriptionRuntimeInfo(String topicPath, String subscriptionName)
    {
        this.topicPath = topicPath;
        this.subscriptionName = subscriptionName;
    }

    /**
     * The path of the topic.
     */
    public String getTopicPath() {
        return topicPath;
    }

    void setTopicPath(String path) {
        this.topicPath = path;
    }

    /**
     * The name of the subscription
     */
    public String getSubscriptionName() {
        return subscriptionName;
    }

    void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    /**
     * The total number of messages in the entity.
     */
    public long getMessageCount() {
        return messageCount;
    }

    void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }

    /**
     * Message count details of the sub-queues of the entity.
     */
    public MessageCountDetails getMessageCountDetails() {
        return messageCountDetails;
    }

    void setMessageCountDetails(MessageCountDetails messageCountDetails) {
        this.messageCountDetails = messageCountDetails;
    }

    /**
     * The date-time when entity was created.
     */
    public Instant getCreatedAt() {
        return createdAt;
    }

    void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * The date-time when the entity was updated
     */
    public Instant getUpdatedAt() {
        return updatedAt;
    }

    void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    /**
     * The date-time when the entity was last accessed.
     */
    public Instant getAccessedAt() {
        return accessedAt;
    }

    void setAccessedAt(Instant accessedAt) {
        this.accessedAt = accessedAt;
    }
}
