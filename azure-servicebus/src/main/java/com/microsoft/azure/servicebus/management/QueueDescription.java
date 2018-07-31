package com.microsoft.azure.servicebus.management;

import org.apache.commons.collections4.CollectionUtils;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;

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
    EntityStatus status = EntityStatus.Active;
    List<AuthorizationRule> authorizationRules = null;

    public QueueDescription(String path)
    {
        this.setPath(path);
    }

    public String getPath()
    {
        return this.path;
    }

    public void setPath(String path)
    {
        EntityNameHelper.checkValidQueueName(path);
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

    public EntityStatus getEntityStatus() {
        return this.status;
    }

    public void setEntityStatus(EntityStatus stats) {
        this.status = status;
    }

    public List<AuthorizationRule> getAuthorizationRules() {
        return authorizationRules;
    }

    public void setAuthorizationRules(List<AuthorizationRule> authorizationRules) {
        this.authorizationRules = authorizationRules;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof QueueDescription)) {
            return false;
        }

        QueueDescription other = (QueueDescription) o;
        if (this.path.equalsIgnoreCase(other.path)
                && this.autoDeleteOnIdle.equals(other.autoDeleteOnIdle)
                && this.defaultMessageTimeToLive.equals(other.defaultMessageTimeToLive)
                && (!this.requiresDuplicateDetection || this.duplicationDetectionHistoryTimeWindow.equals(other.duplicationDetectionHistoryTimeWindow))
                && this.enableBatchedOperations == other.enableBatchedOperations
                && this.enableDeadLetteringOnMessageExpiration == other.enableDeadLetteringOnMessageExpiration
                && this.enablePartitioning == other.enablePartitioning
                && ((this.forwardTo == null && other.forwardTo == null) || this.forwardTo.equalsIgnoreCase(other.forwardTo))
                && ((this.forwardDeadLetteredMessagesTo == null && other.forwardDeadLetteredMessagesTo == null) || this.forwardDeadLetteredMessagesTo.equalsIgnoreCase(other.forwardDeadLetteredMessagesTo))
                && this.lockDuration.equals(other.lockDuration)
                && this.maxDeliveryCount == other.maxDeliveryCount
                && this.maxSizeInMB == other.maxSizeInMB
                && this.requiresDuplicateDetection == other.requiresDuplicateDetection
                && this.requiresSession == other.requiresSession
                && this.status.equals(other.status)
                && ((this.userMetadata == null && other.userMetadata == null) || this.userMetadata.equals(other.userMetadata))
                && AuthorizationRuleUtil.equals(this.authorizationRules, other.authorizationRules)) {
            return true;
        }

        return false;
    }
}
