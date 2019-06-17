// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.servicebus;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

final public class Message implements Serializable, IMessage {
	private static final long serialVersionUID = 7849508139219590863L;
	private static final Charset DEFAULT_CHAR_SET = Charset.forName("UTF-8");
	private static final String DEFAULT_CONTENT_TYPE = null;
	
	private static final MessageBody DEFAULT_CONTENT = Utils.fromBinay(new byte[0]);

	private long deliveryCount;
	
	private String messageId;
	
	private Duration timeToLive;
	
	private MessageBody messageBody;
	
	private String contentType;
	
	private String sessionId;
	
	private long sequenceNumber;
	
	private Instant enqueuedTimeUtc;
	
	private Instant scheduledEnqueueTimeUtc;
	
	private Instant lockedUntilUtc;
	
	private Map<String, Object> properties;
	
	private String correlationId;
	
	private String replyToSessionId;
	
	private String label;
	
	private String to;
	
	private String replyTo;
	
	private String partitionKey;

	private String viaPartitionKey;
	
	private String deadLetterSource;
	
	private UUID lockToken;
	
	private byte[] deliveryTag;
	
	/**
	 * Creates an empty message with an empty byte array as body.
	 */
	public Message()
	{
		this(DEFAULT_CONTENT);
	}
	
	/**
	 * Creates a message from a string. For backward compatibility reasons, the string is converted to a byte array and message body type is set to binary.
	 * @param content content of the message.
	 */
	public Message(String content)
	{
		this(content.getBytes(DEFAULT_CHAR_SET));
	}
	
	/**
	 * Creates a message from a byte array. Message body type is set to binary.
	 * @param content content of the message
	 */
	public Message(byte[] content)
	{
		this(Utils.fromBinay(content));
	}
	
	/**
	 * Creates a message from message body.
	 * @param body message body
	 */
	public Message(MessageBody body)
	{
		this(body, DEFAULT_CONTENT_TYPE);
	}
	
	/**
	 * Creates a message from a string. For backward compatibility reasons, the string is converted to a byte array and message body type is set to binary.
	 * @param content content of the message
	 * @param contentType content type of the message
	 */
	public Message(String content, String contentType)
	{
		this(content.getBytes(DEFAULT_CHAR_SET), contentType);
	}
	
	/**
	 * Creates a message from a byte array. Message body type is set to binary.
	 * @param content content of the message
	 * @param contentType content type of the message
	 */
	public Message(byte[] content, String contentType)
	{
		this(Utils.fromBinay(content), contentType);
	}
	
	/**
	 * Creates a message from message body.
	 * @param body message body
	 * @param contentType content type of the message
	 */
	public Message(MessageBody body, String contentType)
	{
		this(UUID.randomUUID().toString(), body, contentType);
	}
	
	/**
	 * Creates a message from a string. For backward compatibility reasons, the string is converted to a byte array and message body type is set to binary.
	 * @param messageId id of the message
	 * @param content content of the message
	 * @param contentType content type of the message
	 */
	public Message(String messageId, String content, String contentType)
	{
		this(messageId, content.getBytes(DEFAULT_CHAR_SET), contentType);
	}
	
	/**
	 * Creates a message from a byte array. Message body type is set to binary.
	 * @param messageId id of the message
	 * @param content content of the message
	 * @param contentType content type of the message
	 */
	public Message(String messageId, byte[] content, String contentType)
	{
		this(messageId, Utils.fromBinay(content), contentType);
	}

	/**
	 * Creates a message from message body.
	 * @param messageId id of the message
	 * @param body message body
	 * @param contentType content type of the message
	 */
	public Message(String messageId, MessageBody body, String contentType)
	{
		this.messageId = messageId;
		this.messageBody = body;
		this.contentType = contentType;
		this.properties = new HashMap<>();
	}

	@Override
	public long getDeliveryCount() {
		return deliveryCount;
	}

	void setDeliveryCount(long deliveryCount) {
		this.deliveryCount = deliveryCount;
	}

	@Override
	public String getMessageId() {
		return messageId;
	}

	@Override
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	@Override
	public Duration getTimeToLive() {
		return timeToLive;
	}

	@Override
	public void setTimeToLive(Duration timeToLive) {
		this.timeToLive = timeToLive;
	}	

	@Override
	public String getContentType() {
		return this.contentType;
	}

	@Override
	public void setContentType(String contentType) {
		this.contentType = contentType;		
	}

	@Override
	public Instant getExpiresAtUtc() {
		return this.enqueuedTimeUtc.plus(this.timeToLive);
	}

	@Override
	public Instant getLockedUntilUtc() {
		return this.lockedUntilUtc;
	}
	
	public void setLockedUntilUtc(Instant lockedUntilUtc) {
		this.lockedUntilUtc = lockedUntilUtc;
	}

	@Override
	public Instant getEnqueuedTimeUtc() {
		return this.enqueuedTimeUtc;
	}
	
	void setEnqueuedTimeUtc(Instant enqueuedTimeUtc) {
		this.enqueuedTimeUtc = enqueuedTimeUtc;
	}

	@Override
	public long getSequenceNumber() {
		return this.sequenceNumber;
	}
	
	void setSequenceNumber(long sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	@Override
	public String getSessionId() {
		return this.sessionId;
	}

	@Override
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;		
	}
	
	@Override
	public Map<String, Object> getProperties() {
		return this.properties;
	}

	@Override
	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;				
	}

	@Override
	public String getCorrelationId() {
		return this.correlationId;
	}

	@Override
	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}

	@Override
	public String getTo() {
		return this.to;
	}

	@Override
	public void setTo(String to) {
		this.to= to;		
	}

	@Override
	public String getReplyTo() {
		return this.replyTo;
	}

	@Override
	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;		
	}

	@Override
	public String getLabel() {
		return this.label;
	}

	@Override
	public void setLabel(String label) {
		this.label = label;		
	}

	@Override
	public String getReplyToSessionId() {
		return this.replyToSessionId;
	}

	@Override
	public void setReplyToSessionId(String replyToSessionId) {
		this.replyToSessionId = replyToSessionId;		
	}

	@Deprecated
	@Override
	public Instant getScheduledEnqueuedTimeUtc() {
		return this.getScheduledEnqueueTimeUtc();
	}

	@Deprecated
	@Override
	public void setScheduledEnqueuedTimeUtc(Instant scheduledEnqueueTimeUtc) {
		this.setScheduledEnqueueTimeUtc(scheduledEnqueueTimeUtc);
	}
	
	@Override
	public Instant getScheduledEnqueueTimeUtc() {
		return this.scheduledEnqueueTimeUtc;
	}

	@Override
	public void setScheduledEnqueueTimeUtc(Instant scheduledEnqueueTimeUtc) {
		this.scheduledEnqueueTimeUtc = scheduledEnqueueTimeUtc;		
	}

	@Override
	public String getPartitionKey() {
		return this.partitionKey;
	}

	@Override
	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;		
	}

	@Override
	public String getViaPartitionKey() {
        return this.viaPartitionKey;
    }

    @Override
    public void setViaPartitionKey(String partitionKey) {
        this.viaPartitionKey = partitionKey;
    }

	@Override
	public String getDeadLetterSource() {
		return this.deadLetterSource;
	}
	
	void setDeadLetterSource(String deadLetterSource) {
		this.deadLetterSource = deadLetterSource;
	}

	@Override
	public UUID getLockToken() {
		return this.lockToken;
	}
	
	void setLockToken(UUID lockToken){
		this.lockToken = lockToken;
	}
	
	byte[] getDeliveryTag()
	{
		return this.deliveryTag;
	}
	
	void setDeliveryTag(byte[] deliveryTag)
	{
		this.deliveryTag = deliveryTag;
	}

	@Override
	@Deprecated
	public byte[] getBody()
	{
		return Utils.getDataFromMessageBody(this.messageBody);
	}

	@Override
	@Deprecated
	public void setBody(byte[] body)
	{
		this.messageBody = Utils.fromBinay(body);
	}

	@Override
	public MessageBody getMessageBody()
	{
		return this.messageBody;
	}

	@Override
	public void setMessageBody(MessageBody body)
	{
		this.messageBody = body;
	}

	@Override
	public IMessage createCopy() {
		Message copy = new Message(this.getMessageBody(), this.getContentType());
		copy.setProperties(this.getProperties()); // Retain the same properties
		
		copy.setMessageId(this.getMessageId());
		copy.setCorrelationId(this.getCorrelationId());
		copy.setTo(this.getTo());
		copy.setReplyTo(this.getReplyTo());
		copy.setLabel(this.getLabel());
		copy.setReplyToSessionId(this.getReplyToSessionId());
		copy.setSessionId(this.getSessionId());
		copy.setScheduledEnqueueTimeUtc(this.getScheduledEnqueueTimeUtc());
		copy.setPartitionKey(this.getPartitionKey());
		copy.setTimeToLive(this.getTimeToLive());
		
		return copy;
	}
	
		@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
			+ ((contentType == null) ? 0 : contentType.hashCode());
		result = prime * result
			+ ((correlationId == null) ? 0 : correlationId.hashCode());
		result = prime * result
			+ ((deadLetterSource == null) ? 0 : deadLetterSource.hashCode());
		result = prime * result
			+ (int) (deliveryCount ^ (deliveryCount >>> 32));
		result = prime * result + Arrays.hashCode(deliveryTag);
		result = prime * result
			+ ((enqueuedTimeUtc == null) ? 0 : enqueuedTimeUtc.hashCode());
		result = prime * result + ((label == null) ? 0 : label.hashCode());
		result = prime * result
			+ ((lockToken == null) ? 0 : lockToken.hashCode());
		result = prime * result
			+ ((lockedUntilUtc == null) ? 0 : lockedUntilUtc.hashCode());
		result = prime * result
			+ ((messageBody == null) ? 0 : messageBody.hashCode());
		result = prime * result
			+ ((messageId == null) ? 0 : messageId.hashCode());
		result = prime * result
			+ ((partitionKey == null) ? 0 : partitionKey.hashCode());
		result = prime * result
			+ ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((replyTo == null) ? 0 : replyTo.hashCode());
		result = prime * result
			+ ((replyToSessionId == null) ? 0 : replyToSessionId.hashCode());
		result = prime * result + ((scheduledEnqueueTimeUtc == null) ? 0
			: scheduledEnqueueTimeUtc.hashCode());
		result = prime * result
			+ (int) (sequenceNumber ^ (sequenceNumber >>> 32));
		result = prime * result
			+ ((sessionId == null) ? 0 : sessionId.hashCode());
		result = prime * result
			+ ((timeToLive == null) ? 0 : timeToLive.hashCode());
		result = prime * result + ((to == null) ? 0 : to.hashCode());
		result = prime * result
			+ ((viaPartitionKey == null) ? 0 : viaPartitionKey.hashCode());
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
		Message other = (Message) obj;
		if (contentType == null) {
			if (other.contentType != null)
				return false;
		}
		else if (!contentType.equals(other.contentType))
			return false;
		if (correlationId == null) {
			if (other.correlationId != null)
				return false;
		}
		else if (!correlationId.equals(other.correlationId))
			return false;
		if (deadLetterSource == null) {
			if (other.deadLetterSource != null)
				return false;
		}
		else if (!deadLetterSource.equals(other.deadLetterSource))
			return false;
		if (deliveryCount != other.deliveryCount)
			return false;
		if (!Arrays.equals(deliveryTag, other.deliveryTag))
			return false;
		if (enqueuedTimeUtc == null) {
			if (other.enqueuedTimeUtc != null)
				return false;
		}
		else if (!enqueuedTimeUtc.equals(other.enqueuedTimeUtc))
			return false;
		if (label == null) {
			if (other.label != null)
				return false;
		}
		else if (!label.equals(other.label))
			return false;
		if (lockToken == null) {
			if (other.lockToken != null)
				return false;
		}
		else if (!lockToken.equals(other.lockToken))
			return false;
		if (lockedUntilUtc == null) {
			if (other.lockedUntilUtc != null)
				return false;
		}
		else if (!lockedUntilUtc.equals(other.lockedUntilUtc))
			return false;
		if (messageBody == null) {
			if (other.messageBody != null)
				return false;
		}
		else if (!messageBody.equals(other.messageBody))
			return false;
		if (messageId == null) {
			if (other.messageId != null)
				return false;
		}
		else if (!messageId.equals(other.messageId))
			return false;
		if (partitionKey == null) {
			if (other.partitionKey != null)
				return false;
		}
		else if (!partitionKey.equals(other.partitionKey))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		}
		else if (!properties.equals(other.properties))
			return false;
		if (replyTo == null) {
			if (other.replyTo != null)
				return false;
		}
		else if (!replyTo.equals(other.replyTo))
			return false;
		if (replyToSessionId == null) {
			if (other.replyToSessionId != null)
				return false;
		}
		else if (!replyToSessionId.equals(other.replyToSessionId))
			return false;
		if (scheduledEnqueueTimeUtc == null) {
			if (other.scheduledEnqueueTimeUtc != null)
				return false;
		}
		else if (!scheduledEnqueueTimeUtc.equals(other.scheduledEnqueueTimeUtc))
			return false;
		if (sequenceNumber != other.sequenceNumber)
			return false;
		if (sessionId == null) {
			if (other.sessionId != null)
				return false;
		}
		else if (!sessionId.equals(other.sessionId))
			return false;
		if (timeToLive == null) {
			if (other.timeToLive != null)
				return false;
		}
		else if (!timeToLive.equals(other.timeToLive))
			return false;
		if (to == null) {
			if (other.to != null)
				return false;
		}
		else if (!to.equals(other.to))
			return false;
		if (viaPartitionKey == null) {
			if (other.viaPartitionKey != null)
				return false;
		}
		else if (!viaPartitionKey.equals(other.viaPartitionKey))
			return false;
		return true;
	}
}
