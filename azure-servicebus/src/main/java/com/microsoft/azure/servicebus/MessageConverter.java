// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.servicebus;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;

import com.microsoft.azure.servicebus.primitives.ClientConstants;
import com.microsoft.azure.servicebus.primitives.MessageWithDeliveryTag;
import com.microsoft.azure.servicebus.primitives.MessageWithLockToken;
import com.microsoft.azure.servicebus.primitives.StringUtil;
import com.microsoft.azure.servicebus.primitives.Util;

class MessageConverter
{	
	public static org.apache.qpid.proton.message.Message convertBrokeredMessageToAmqpMessage(Message brokeredMessage)	
	{
		org.apache.qpid.proton.message.Message amqpMessage = Proton.message();
		MessageBody body = brokeredMessage.getMessageBody();
		if( body != null)
		{
		    if (body.getBodyType() == MessageBodyType.VALUE)
		    {
		        amqpMessage.setBody(new AmqpValue(body.getValueData()));
		    }
		    else if (body.getBodyType() == MessageBodyType.SEQUENCE)
		    {
		        amqpMessage.setBody(new AmqpSequence(Utils.getSequenceFromMessageBody(body)));
		    }
		    else
		    {
		        amqpMessage.setBody(new Data(new Binary(Utils.getDataFromMessageBody(body))));
		    }
		}
		
		if(brokeredMessage.getProperties() != null)
		{
			Map<String, Object> applicationProperties = new HashMap<>();
			applicationProperties.putAll(brokeredMessage.getProperties());
			amqpMessage.setApplicationProperties(new ApplicationProperties(applicationProperties));
		}
		
		if(brokeredMessage.getTimeToLive() != null)
		{
			amqpMessage.setTtl(brokeredMessage.getTimeToLive().toMillis());
		}		
		
		amqpMessage.setMessageId(brokeredMessage.getMessageId());
		amqpMessage.setContentType(brokeredMessage.getContentType());
		amqpMessage.setCorrelationId(brokeredMessage.getCorrelationId());
		amqpMessage.setSubject(brokeredMessage.getLabel());
		amqpMessage.getProperties().setTo(brokeredMessage.getTo());
		amqpMessage.setReplyTo(brokeredMessage.getReplyTo());
		amqpMessage.setReplyToGroupId(brokeredMessage.getReplyToSessionId());
		amqpMessage.setGroupId(brokeredMessage.getSessionId());
		
		Map<Symbol, Object> messageAnnotationsMap = new HashMap<Symbol, Object>();
		if(brokeredMessage.getScheduledEnqueueTimeUtc() != null)
		{
			messageAnnotationsMap.put(Symbol.valueOf(ClientConstants.SCHEDULEDENQUEUETIMENAME), Date.from(brokeredMessage.getScheduledEnqueueTimeUtc()));
		}
		
		if(!StringUtil.isNullOrEmpty(brokeredMessage.getPartitionKey()))
		{
			messageAnnotationsMap.put(Symbol.valueOf(ClientConstants.PARTITIONKEYNAME), brokeredMessage.getPartitionKey());
		}
		
		amqpMessage.setMessageAnnotations(new MessageAnnotations(messageAnnotationsMap));
		
		return amqpMessage;
	}
	
	public static Message convertAmqpMessageToBrokeredMessage(org.apache.qpid.proton.message.Message amqpMessage)
	{
		return convertAmqpMessageToBrokeredMessage(amqpMessage, (byte[])null);
	}
	
	public static Message convertAmqpMessageToBrokeredMessage(MessageWithDeliveryTag amqpMessageWithDeliveryTag)
	{
		org.apache.qpid.proton.message.Message amqpMessage = amqpMessageWithDeliveryTag.getMessage();
		byte[] deliveryTag = amqpMessageWithDeliveryTag.getDeliveryTag();
		return convertAmqpMessageToBrokeredMessage(amqpMessage, deliveryTag);
	}
	
	public static Message convertAmqpMessageToBrokeredMessage(MessageWithLockToken amqpMessageWithLockToken)
	{
		Message convertedMessage = convertAmqpMessageToBrokeredMessage(amqpMessageWithLockToken.getMessage(), (byte[])null);
		convertedMessage.setLockToken(amqpMessageWithLockToken.getLockToken());
		return convertedMessage;		
	}
		
	public static Message convertAmqpMessageToBrokeredMessage(org.apache.qpid.proton.message.Message amqpMessage, byte[] deliveryTag)
	{		
		Message brokeredMessage;
		Section body = amqpMessage.getBody();
		if(body != null)
		{
			if(body instanceof Data)
			{
				Binary messageData = ((Data)body).getValue();
				brokeredMessage = new Message(Utils.fromBinay(messageData.getArray()));
			}
			else if (body instanceof AmqpValue)
			{
			    Object messageData = ((AmqpValue)body).getValue();
			    brokeredMessage = new Message(MessageBody.fromValueData(messageData));
			}
			else if (body instanceof AmqpSequence)
			{
			    List<Object> messageData = ((AmqpSequence)body).getValue();
			    brokeredMessage = new Message(Utils.fromSequence(messageData));
			}
			else
			{
			    // Should never happen
			    brokeredMessage = new Message();
			}
		}
		else
		{
			brokeredMessage = new Message();
		}
		
		// Application properties
		ApplicationProperties applicationProperties = amqpMessage.getApplicationProperties();
		if(applicationProperties != null)
		{
			Map<String, String> convertedProperties = new HashMap<>();
			for(Map.Entry<String, Object> entry : applicationProperties.getValue().entrySet())
			{
				if(entry.getValue() instanceof String)
				{
					convertedProperties.put(entry.getKey(), (String)entry.getValue());
				}
				else
				{
					if(entry.getValue() == null)
					{
						convertedProperties.put(entry.getKey(), null);
					}
					else
					{
						convertedProperties.put(entry.getKey(), entry.getValue().toString());
					}
				}
			}
			
			brokeredMessage.setProperties(convertedProperties);
		}		
		
		// Header
		brokeredMessage.setTimeToLive(Duration.ofMillis(amqpMessage.getTtl()));
		brokeredMessage.setDeliveryCount(amqpMessage.getDeliveryCount());
		
		// Properties
		Object messageId = amqpMessage.getMessageId();
		if (messageId != null)
		{
			brokeredMessage.setMessageId(messageId.toString());
		}

		brokeredMessage.setContentType(amqpMessage.getContentType());
		Object correlationId = amqpMessage.getCorrelationId();
		if(correlationId != null)
		{
			brokeredMessage.setCorrelationId(correlationId.toString());
		}

		Properties properties = amqpMessage.getProperties();
		if (properties != null)
		{
			brokeredMessage.setTo(properties.getTo());
		}

		brokeredMessage.setLabel(amqpMessage.getSubject());
		brokeredMessage.setReplyTo(amqpMessage.getReplyTo());
		brokeredMessage.setReplyToSessionId(amqpMessage.getReplyToGroupId());
		brokeredMessage.setSessionId(amqpMessage.getGroupId());
		
		// Message Annotations
		MessageAnnotations messageAnnotations = amqpMessage.getMessageAnnotations();
		if(messageAnnotations != null)
		{
			Map<Symbol, Object> messageAnnotationsMap = messageAnnotations.getValue();
			if(messageAnnotationsMap != null)
			{
				for(Map.Entry<Symbol, Object> entry : messageAnnotationsMap.entrySet())
				{
					String entryName = entry.getKey().toString();
					switch(entryName)
					{
						case ClientConstants.ENQUEUEDTIMEUTCNAME:
							brokeredMessage.setEnqueuedTimeUtc(((Date)entry.getValue()).toInstant());
							break;
						case ClientConstants.SCHEDULEDENQUEUETIMENAME:
	                        brokeredMessage.setScheduledEnqueueTimeUtc(((Date)entry.getValue()).toInstant());
	                        break;
	                    case ClientConstants.SEQUENCENUBMERNAME:
	                        brokeredMessage.setSequenceNumber((long)entry.getValue());
	                        break;
	                    case ClientConstants.LOCKEDUNTILNAME:
	                        brokeredMessage.setLockedUntilUtc(((Date)entry.getValue()).toInstant());
	                        break;
	                    case ClientConstants.PARTITIONKEYNAME:
	                        brokeredMessage.setPartitionKey((String)entry.getValue());
	                        break;
	                    case ClientConstants.DEADLETTERSOURCENAME:
	                        brokeredMessage.setDeadLetterSource((String)entry.getValue());
	                        break;
					}				
				}
			}
		}
		
		if(deliveryTag != null && deliveryTag.length == ClientConstants.LOCKTOKENSIZE)
		{
			UUID lockToken = Util.convertDotNetBytesToUUID(deliveryTag);
			brokeredMessage.setLockToken(lockToken);
		}
		else
		{
			brokeredMessage.setLockToken(ClientConstants.ZEROLOCKTOKEN);
		}
		
		brokeredMessage.setDeliveryTag(deliveryTag);
		
		return brokeredMessage;
	}
}
