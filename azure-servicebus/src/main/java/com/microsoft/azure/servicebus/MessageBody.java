package com.microsoft.azure.servicebus;

import java.util.List;

/**
 * This class encapsulates the body of a message. Body types map to AMQP message body types.
 * It has getters and setters for multiple body types.
 * Client should test for body type before calling corresponding get method.
 * Get methods not corresponding to the type of the body return null.
 *
 */
public class MessageBody {
    private MessageBodyType bodyType;
    private Object value;
    private List<Object> sequence;
    private byte[] binaryData;
    
    /**
     * Creates message body of Value type.
     * @param value content of the message. It must be of a type supported by AMQP.
     */
    public MessageBody(Object value)
    {
        this.bodyType = MessageBodyType.VALUE;
        this.value = value;
        this.sequence = null;
        this.binaryData = null;
    }
    
    /**
     * Creates a message body of Sequence type.
     * @param sequence content of the message. Each object in the sequence must of a type supported by AMQP.
     */
    public MessageBody(List<Object> sequence)
    {
        this.bodyType = MessageBodyType.SEQUENCE;
        this.value = null;
        this.sequence = sequence;
        this.binaryData = null;
    }
    
    /**
     * Creates a message body of Binary type.
     * @param binaryData content of the message.
     */
    public MessageBody(byte[] binaryData)
    {
        this.bodyType = MessageBodyType.BINARY;
        this.value = null;
        this.sequence = null;
        this.binaryData = binaryData;
    }
    
    /**
     * Returns the content of message body.
     * @return value of message body only if the MessageBody is of Value type. Returns null otherwise.
     */
    public Object getValue() {
        return value;
    }
    
    /**
     * Returns the content of message body.
     * @return a list of objects only if the MessageBody is of Sequence type. Returns null otherwise.
     */
    public List<Object> getSequence() {
        return sequence;
    }
    
    /**
     * Returns the content of message body.
     * @return message body as byte array only if the MessageBody is of Binary type. Returns null otherwise.
     */
    public byte[] getBinaryData() {
        return binaryData;
    }
    
    /**
     * Return the type of content in this message body.
     * @return type of message content
     */
    public MessageBodyType getBodyType() {
        return bodyType;
    }
}
