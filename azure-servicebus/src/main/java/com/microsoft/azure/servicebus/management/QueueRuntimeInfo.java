package com.microsoft.azure.servicebus.management;

import java.time.Instant;

/**
 * This provides runtime information of the queue.
 */
public class QueueRuntimeInfo {
    private String path;
    private long messageCount;
    private MessageCountDetails messageCountDetails;
    private long sizeInBytes;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant accessedAt;

    QueueRuntimeInfo(String path)
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
}
