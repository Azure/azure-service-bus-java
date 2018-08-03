package com.microsoft.azure.servicebus.management;

import java.time.Instant;

/**
 * This provides runtime information of the topic.
 */
public class TopicRuntimeInfo {
    private String path;
    private MessageCountDetails messageCountDetails;
    private long sizeInBytes;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant accessedAt;
    private int subscriptionCount;

    TopicRuntimeInfo(String path)
    {
        this.path = path;
    }

    /**
     * The path of the entity.
     */
    public String getPath() {
        return path;
    }

    void setPath(String path) {
        this.path = path;
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
     * Current size of the entity in bytes.
     */
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    void setSizeInBytes(long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
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

    /**
     * Number of subscriptions on the topic.
     */
    public int getSubscriptionCount() {
        return subscriptionCount;
    }

    void setSubscriptionCount(int subscriptionCount) {
        this.subscriptionCount = subscriptionCount;
    }
}
