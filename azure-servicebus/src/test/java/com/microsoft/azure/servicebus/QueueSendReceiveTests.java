package com.microsoft.azure.servicebus;

import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

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

    @Test
    public void transactionalSendCommitTest() throws ServiceBusException, InterruptedException, ExecutionException {
        this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.receiveEntityPath, ReceiveMode.PEEKLOCK);

        ByteBuffer txnId = this.factory.startTransaction().get();
        Assert.assertNotNull(txnId);

        String messageId = UUID.randomUUID().toString();
        Message message = new Message("AMQP message");
        message.setMessageId(messageId);
        this.sender.send(message, txnId);

        this.factory.endTransaction(txnId, true).get();

        IMessage receivedMessage = this.receiver.receive(TestCommons.SHORT_WAIT_TIME);
        Assert.assertNotNull("Message not received", receivedMessage);
        Assert.assertEquals("Message Id did not match", messageId, receivedMessage.getMessageId());

        this.receiver.complete(receivedMessage.getLockToken());
    }

    @Test
    public void transactionalSendRollbackTest() throws ServiceBusException, InterruptedException, ExecutionException {
        this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.receiveEntityPath, ReceiveMode.PEEKLOCK);

        ByteBuffer txnId = this.factory.startTransaction().get();
        Assert.assertNotNull(txnId);

        String messageId = UUID.randomUUID().toString();
        Message message = new Message("AMQP message");
        message.setMessageId(messageId);
        this.sender.send(message, txnId);

        this.factory.endTransaction(txnId, false).get();

        IMessage receivedMessage = this.receiver.receive(TestCommons.SHORT_WAIT_TIME);
        Assert.assertNull(receivedMessage);
    }

    @Test
    public void transactionalCompleteCommitTest() throws ServiceBusException, InterruptedException, ExecutionException {
        this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.receiveEntityPath, ReceiveMode.PEEKLOCK);

        Message message = new Message("AMQP message");
        this.sender.send(message);
        IMessage receivedMessage = this.receiver.receive(TestCommons.SHORT_WAIT_TIME);

        ByteBuffer txnId = this.factory.startTransaction().get();
        this.receiver.complete(receivedMessage.getLockToken(), txnId);
        this.factory.endTransaction(txnId, true).get();

        receivedMessage = this.receiver.receive(TestCommons.SHORT_WAIT_TIME);
        Assert.assertNull(receivedMessage);
    }

    @Ignore("Takes a long time")
    @Test
    public void transactionalCompleteRollbackTest() throws ServiceBusException, InterruptedException, ExecutionException {
        this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.receiveEntityPath, ReceiveMode.PEEKLOCK);

        Message message = new Message("AMQP message");
        this.sender.send(message);

        IMessage receivedMessage = this.receiver.receive(TestCommons.SHORT_WAIT_TIME);

        ByteBuffer txnId = this.factory.startTransaction().get();
        Assert.assertNotNull(txnId);
        this.receiver.complete(receivedMessage.getLockToken(), txnId);
        this.factory.endTransaction(txnId, false).get();

        Thread.sleep(1000 * 40);    // Waiting for lock to expire. TODO: Find a better way to handle this.

        receivedMessage = this.receiver.receive();
        Assert.assertNotNull(txnId);
        this.receiver.complete(receivedMessage.getLockToken());
    }

    @Test
    public void transactionalRequestResponseDispositionTest() throws ServiceBusException, InterruptedException, ExecutionException {
        this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.receiveEntityPath, ReceiveMode.PEEKLOCK);

        Message message = new Message("AMQP message");
        this.sender.send(message);
        IMessage receivedMessage = this.receiver.receive(TestCommons.SHORT_WAIT_TIME);
        this.receiver.defer(receivedMessage.getLockToken());
        receivedMessage = this.receiver.receiveDeferredMessage(receivedMessage.getSequenceNumber());
        Assert.assertNotNull(receivedMessage);

        ByteBuffer txnId = this.factory.startTransaction().get();
        this.receiver.complete(receivedMessage.getLockToken(), txnId);
        this.factory.endTransaction(txnId, true).get();

        receivedMessage = this.receiver.receive(TestCommons.SHORT_WAIT_TIME);
        Assert.assertNull(receivedMessage);
    }

    @Test
    public void transactionalSendRollbackTest2() throws ServiceBusException, InterruptedException, ExecutionException {
        this.sender = ClientFactory.createMessageSenderFromEntityPath(this.factory, this.entityName);
        this.receiver = ClientFactory.createMessageReceiverFromEntityPath(factory, this.receiveEntityPath, ReceiveMode.PEEKLOCK);

        ByteBuffer txnId = this.factory.startTransaction().get();
        Assert.assertNotNull(txnId);

        String messageId = UUID.randomUUID().toString();
        Message message = new Message("AMQP message");
        message.setMessageId(messageId);
        this.sender.send(message, txnId);

        this.factory.endTransaction(txnId, true).get();

        IMessage receivedMessage = this.receiver.receive(TestCommons.SHORT_WAIT_TIME);

        //Assert.assertNull(receivedMessage);
        Assert.assertNotNull("Message not received", receivedMessage);
        Assert.assertEquals("Message Id did not match", messageId, receivedMessage.getMessageId());

        txnId = this.factory.startTransaction().get();
        Assert.assertNotNull(txnId);
        System.out.println("Declared " + new String(txnId.array(), txnId.position(), txnId.limit()));

        System.out.println("Completing");
        this.receiver.complete(receivedMessage.getLockToken(), txnId);
        //this.receiver.complete(receivedMessage.getLockToken());

        System.out.println("Discharging");
        this.factory.endTransaction(txnId, true).get();

        //receivedMessage = this.receiver.receive(TestCommons.SHORT_WAIT_TIME);
        //Assert.assertNull(receivedMessage);
    }
}
