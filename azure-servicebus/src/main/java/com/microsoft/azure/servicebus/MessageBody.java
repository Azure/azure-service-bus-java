package com.microsoft.azure.servicebus;

import java.util.List;

public class MessageBody {
    private MessageBodyType bodyType;
    private Object value;
    private List<Object> sequence;
    private byte[] binaryData;
    
    public MessageBody(Object value)
    {
        this.bodyType = MessageBodyType.VALUE;
        this.value = value;
        this.sequence = null;
        this.binaryData = null;
    }
    
    public MessageBody(List<Object> sequence)
    {
        this.bodyType = MessageBodyType.SEQUENCE;
        this.value = null;
        this.sequence = sequence;
        this.binaryData = null;
    }
    
    public MessageBody(byte[] binaryData)
    {
        this.bodyType = MessageBodyType.BINARY;
        this.value = null;
        this.sequence = null;
        this.binaryData = binaryData;
    }
    
    public Object getValue() {
        return value;
    }
    
    public List<Object> getSequence() {
        return sequence;
    }
    
    public byte[] getBinaryData() {
        return binaryData;
    }
    
    public MessageBodyType getBodyType() {
        return bodyType;
    }
}
