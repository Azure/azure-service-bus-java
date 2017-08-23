package com.microsoft.azure.servicebus.management;

import java.time.Duration;

public class TopicDescription {
    private String path;
    private int maxSizeInMegaBytes;
    private boolean enablePartitioning;
    private boolean enableSubscriptionPartitioning;
    private boolean requiresDuplicateDetection;
    private boolean enableExpress;
    private boolean supportsOrdering;
    private boolean enableFilteringMessagesBeforePublishing;
    private Duration autoDeleteOnIdle;
    private Duration defaultMessageTimeToLive;
    private Duration duplicateDetectionHistoryTimeWindow;
    
    public TopicDescription(String path)
    {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getMaxSizeInMegaBytes() {
        return maxSizeInMegaBytes;
    }

    public void setMaxSizeInMegaBytes(int maxSizeInMegaBytes) {
        this.maxSizeInMegaBytes = maxSizeInMegaBytes;
    }

    public boolean isEnablePartitioning() {
        return enablePartitioning;
    }

    public void setEnablePartitioning(boolean enablePartitioning) {
        this.enablePartitioning = enablePartitioning;
    }
}
