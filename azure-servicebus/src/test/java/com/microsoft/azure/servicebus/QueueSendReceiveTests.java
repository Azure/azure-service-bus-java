package com.microsoft.azure.servicebus;

public class QueueSendReceiveTests extends SendReceiveTests
{
    @Override
    public String getEntityNamePrefix() {
       return "QueueSendReceiveTests";
    }

    @Override
    public boolean isEntityQueue() {
        return true;
    }

    @Override
    public boolean shouldCreateEntityForEveryTest() {
        return TestUtils.shouldCreateEntityForEveryTest();
    }

    @Override
    public boolean isEntityPartitioned() {
        return false;
    }
        if(this.isEntityPartitioned())
        {
        	message.setPartitionKey(messageId);
        }
        if(this.isEntityPartitioned())
        {
        	message.setPartitionKey(messageId);
        }
        
        if(this.isEntityPartitioned())
        {
        	message2.setPartitionKey(receivedMessage.getPartitionKey());
        }
}
