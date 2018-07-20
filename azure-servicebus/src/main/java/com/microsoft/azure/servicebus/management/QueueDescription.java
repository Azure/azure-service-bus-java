package com.microsoft.azure.servicebus.management;

import java.time.Duration;

public class QueueDescription {
    Duration duplicationDetectionHistoryTimeWindow = Duration.ofMinutes(1);
    String path;
    Duration lockDuration = Duration.ofSeconds(60);
    Duration defaultMessageTimeToLive = ManagementClientConstants.MAX_DURATION;
    Duration autoDeleteOnIdle = ManagementClientConstants.MAX_DURATION;
    int maxDeliveryCount = 10;
    String forwardTo = null;
    String forwardDeadLetteredMessagesTo = null;
    String userMetadata = null;
    long maxSizeInMB = 1024;
    boolean requiresDuplicateDetection = false;
    boolean enableDeadLetteringOnMessageExpiration = false;
    boolean requiresSession = false;
    boolean enableBatchedOperations = true;
    boolean enablePartitioning = false;

    // TODO: AuthorizationRules
    // TODO: Status

    public QueueDescription(String path)
    {
        this.path = path;
    }

    public String getPath()
    {
        return this.path;
    }

    public void setPath(String path)
    {
        //TODO: EntityNameHelper.CheckValidQueueName
        this.path = path;
    }

    public Duration getLockDuration()
    {
        return this.lockDuration;
    }

    public void setLockDuration(Duration lockDuration)
    {
        // TODO: TimeoutHelper.ThrowIfNonPositiveArgument(value, nameof(LockDuration));
        this.lockDuration = lockDuration;
    }

    public long getMaxSizeInMB() {
        return this.maxSizeInMB;
    }

    public void setMaxSizeInMB(long maxSize)
    {
        this.maxSizeInMB = maxSize;
    }

    public boolean isRequiresDuplicateDetection() {
        return requiresDuplicateDetection;
    }

    public void setRequiresDuplicateDetection(boolean requiresDuplicateDetection) {
        this.requiresDuplicateDetection = requiresDuplicateDetection;
    }

    public boolean isRequiresSession() {
        return requiresSession;
    }

    public void setRequiresSession(boolean requiresSession) {
        this.requiresSession = requiresSession;
    }

    public Duration getDefaultMessageTimeToLive() {
        return defaultMessageTimeToLive;
    }

    public void setDefaultMessageTimeToLive(Duration defaultMessageTimeToLive) {
        if (defaultMessageTimeToLive != null &&
                (defaultMessageTimeToLive.compareTo(ManagementClientConstants.MIN_ALLOWED_TTL) < 0 ||
                defaultMessageTimeToLive.compareTo(ManagementClientConstants.MAX_ALLOWED_TTL) > 0))
        {
            throw new IllegalArgumentException(
                    String.format("The value must be between %s and %s.",
                    ManagementClientConstants.MAX_ALLOWED_TTL,
                            ManagementClientConstants.MIN_ALLOWED_TTL));
        }

        this.defaultMessageTimeToLive = defaultMessageTimeToLive;
    }

    public Duration getAutoDeleteOnIdle() {
        return autoDeleteOnIdle;
    }

    public void setAutoDeleteOnIdle(Duration autoDeleteOnIdle) {
        if (autoDeleteOnIdle != null &&
            autoDeleteOnIdle.compareTo(ManagementClientConstants.MIN_ALLOWED_AUTODELETE_DURATION) < 0)
        {
            throw new IllegalArgumentException(
                    String.format("The value must be greater than %s.",
                            ManagementClientConstants.MIN_ALLOWED_AUTODELETE_DURATION));
        }

        this.autoDeleteOnIdle = autoDeleteOnIdle;
    }

    public boolean isEnableDeadLetteringOnMessageExpiration() {
        return enableDeadLetteringOnMessageExpiration;
    }

    public void setEnableDeadLetteringOnMessageExpiration(boolean enableDeadLetteringOnMessageExpiration) {
        this.enableDeadLetteringOnMessageExpiration = enableDeadLetteringOnMessageExpiration;
    }

    public Duration getDuplicationDetectionHistoryTimeWindow() {
        return duplicationDetectionHistoryTimeWindow;
    }

    public void setDuplicationDetectionHistoryTimeWindow(Duration duplicationDetectionHistoryTimeWindow) {
        if (duplicationDetectionHistoryTimeWindow != null &&
                (duplicationDetectionHistoryTimeWindow.compareTo(ManagementClientConstants.MIN_DUPLICATE_HISTORY_DURATION) < 0 ||
                        duplicationDetectionHistoryTimeWindow.compareTo(ManagementClientConstants.MAX_DUPLICATE_HISTORY_DURATION) > 0))
        {
            throw new IllegalArgumentException(
                    String.format("The value must be between %s and %s.",
                            ManagementClientConstants.MIN_DUPLICATE_HISTORY_DURATION,
                            ManagementClientConstants.MAX_DUPLICATE_HISTORY_DURATION));
        }

        this.duplicationDetectionHistoryTimeWindow = duplicationDetectionHistoryTimeWindow;
    }

    public int getMaxDeliveryCount() {
        return maxDeliveryCount;
    }

    public void setMaxDeliveryCount(int maxDeliveryCount) {
        if (maxDeliveryCount < ManagementClientConstants.MIN_ALLOWED_MAX_DELIVERYCOUNT)
        {
            throw new IllegalArgumentException(
                    String.format("The value must be greater than %s.",
                            ManagementClientConstants.MIN_ALLOWED_MAX_DELIVERYCOUNT));
        }

        this.maxDeliveryCount = maxDeliveryCount;
    }

    public boolean isEnableBatchedOperations() {
        return enableBatchedOperations;
    }

    public void setEnableBatchedOperations(boolean enableBatchedOperations) {
        this.enableBatchedOperations = enableBatchedOperations;
    }

    public String getForwardTo() {
        return forwardTo;
    }

    public void setForwardTo(String forwardTo) {
        if (forwardTo == null || forwardTo.isEmpty()) {
            this.forwardTo = forwardTo;
        }

        // TODO:
        // EntityNameHelper.CheckValidQueueName(value, nameof(ForwardTo));
        if (this.path.equals(forwardTo)) {
            throw new IllegalArgumentException("Entity cannot have auto-forwarding policy to itself");
        }

        this.forwardTo = forwardTo;
    }

    public String getForwardDeadLetteredMessagesTo() {
        return forwardDeadLetteredMessagesTo;
    }

    public void setForwardDeadLetteredMessagesTo(String forwardDeadLetteredMessagesTo) {
        if (forwardDeadLetteredMessagesTo == null || forwardDeadLetteredMessagesTo.isEmpty()) {
            this.forwardDeadLetteredMessagesTo = forwardDeadLetteredMessagesTo;
        }

        // TODO:
        // EntityNameHelper.CheckValidQueueName(value, nameof(forwardDeadLetteredMessagesTo));
        if (this.path.equals(forwardDeadLetteredMessagesTo)) {
            throw new IllegalArgumentException("Entity cannot have auto-forwarding policy to itself");
        }

        this.forwardDeadLetteredMessagesTo = forwardDeadLetteredMessagesTo;
    }

    public boolean isEnablePartitioning() {
        return enablePartitioning;
    }

    public void setEnablePartitioning(boolean enablePartitioning) {
        this.enablePartitioning = enablePartitioning;
    }

    public String getUserMetadata() {
        return userMetadata;
    }

    public void setUserMetadata(String userMetadata) {
        if (userMetadata == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        if (userMetadata.length() > ManagementClientConstants.MAX_USERMETADATA_LENGTH) {
            throw new IllegalArgumentException("Length cannot cross " + ManagementClientConstants.MAX_USERMETADATA_LENGTH + " characters");
        }

        this.userMetadata = userMetadata;
    }
}
