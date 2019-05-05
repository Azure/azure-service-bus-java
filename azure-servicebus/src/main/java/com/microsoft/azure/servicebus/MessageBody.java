package com.microsoft.azure.servicebus;

import java.io.Serializable;
import java.util.List;

/**
 * This class encapsulates the body of a message. Body types map to AMQP message body types.
 * It has getters and setters for multiple body types.
 * Client should test for body type before calling corresponding get method.
 * Get methods not corresponding to the type of the body return null.
 */
public class MessageBody implements Serializable{
    
	private static final long serialVersionUID = 7215009530928988502L;
	
	private MessageBodyType bodyType;
    private Object valueData;
    private List<List<Object>> sequenceData;
    private List<byte[]> binaryData;
    
    private MessageBody() {}
    
    /**
     * Creates message body of AMQPValue type.
     * @param value AMQPValue content of the message. It must be of a type supported by AMQP.
     * @return MessageBody instance wrapping around the value data.
     */
    public static MessageBody fromValueData(Object value)
    {
    	if(value == null)
    	{
    		throw new IllegalArgumentException("Value data is null.");
    	}
    	
    	MessageBody body = new MessageBody();
    	body.bodyType = MessageBodyType.VALUE;
    	body.valueData = value;
    	body.sequenceData = null;
    	body.binaryData = null;
    	return body;
    }
    
    /**
     * Creates a message body from a list of AMQPSequence sections.Each AMQPSequence section is in turn a list of objects.
     * Please note that this version of the SDK supports only one AMQPSequence section in a message. It means only a list of exactly one sequence in it is accepted as message body.
     * @param sequenceData a list of AMQPSequence sections. Each AMQPSequence section is in turn a list of objects. Every object in each list must of a type supported by AMQP.
     * @return MessageBody instance wrapping around the sequence data.
     */
    public static MessageBody fromSequenceData(List<List<Object>> sequenceData)
    {
    	if(sequenceData == null || sequenceData.size() == 0 || sequenceData.size() > 1)
    	{
    		throw new IllegalArgumentException("Sequence data is null or has more than one collection in it.");
    	}
    	
    	MessageBody body = new MessageBody();
    	body.bodyType = MessageBodyType.SEQUENCE;
    	body.valueData = null;
    	body.sequenceData = sequenceData;
    	body.binaryData = null;
        return body;
    }
    
    /**
     * Creates a message body from a list of Data sections.Each Data section is a byte array.
     * Please note that this version of the SDK supports only one Data section in a message. It means only a list of exactly one byte array in it is accepted as message body.
     * @param binaryData a list of byte arrays.
     * @return MessageBody instance wrapping around the binary data.
     */
    public static MessageBody fromBinaryData(List<byte[]> binaryData)
    {
    	if(binaryData == null || binaryData.size() == 0 || binaryData.size() > 1)
    	{
    		throw new IllegalArgumentException("Binary data is null or has more than one byte array in it.");
    	}
    	
    	MessageBody body = new MessageBody();
    	body.bodyType = MessageBodyType.BINARY;
    	body.valueData = null;
    	body.sequenceData = null;
    	body.binaryData = binaryData;
        return body;
    }
    
    /**
     * Returns the content of message body.
     * @return value of message body only if the MessageBody is of Value type. Returns null otherwise.
     */
    public Object getValueData() {
        return valueData;
    }
    
    /**
     * Returns the content of message body.
     * @return a list of AMQPSequence sections only if the MessageBody is of Sequence type. Returns null otherwise. Each AMQPSequence section is in turn a list of objects.
     */
    public List<List<Object>> getSequenceData() {
        return sequenceData;
    }
    
    /**
     * Returns the content of message body.
     * @return message body as list of byte arrays only if the MessageBody is of Binary type. Returns null otherwise.
     */
    public List<byte[]> getBinaryData() {
        return binaryData;
    }
    
    /**
     * Return the type of content in this message body.
     * @return type of message content
     */
    public MessageBodyType getBodyType() {
        return bodyType;
    }
	
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
			+ ((binaryData == null) ? 0 : binaryData.hashCode());
		result = prime * result
			+ ((bodyType == null) ? 0 : bodyType.hashCode());
		result = prime * result
			+ ((sequenceData == null) ? 0 : sequenceData.hashCode());
		result = prime * result
			+ ((valueData == null) ? 0 : valueData.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MessageBody other = (MessageBody) obj;
		if (binaryData == null) {
			if (other.binaryData != null)
				return false;
		}
		else if (!binaryData.equals(other.binaryData))
			return false;
		if (bodyType != other.bodyType)
			return false;
		if (sequenceData == null) {
			if (other.sequenceData != null)
				return false;
		}
		else if (!sequenceData.equals(other.sequenceData))
			return false;
		if (valueData == null) {
			if (other.valueData != null)
				return false;
		}
		else if (!valueData.equals(other.valueData))
			return false;
		return true;
	}	
}
