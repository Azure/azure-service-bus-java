package com.microsoft.azure.servicebus.primitives;

import org.apache.qpid.proton.amqp.Binary;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionManager {
    //private static final ThreadLocal<UUID> transactionIdBinary = new ThreadLocal<>();
    //private static final ConcurrentHashMap<UUID, CompletableFuture<Binary>> enlistmentMap = new ConcurrentHashMap<>();
/*
    public static CompletableFuture<Binary> startTransaction(MessagingFactory factory) throws ServiceBusException {
        if (transactionIdBinary.get() != null) {
            throw new ServiceBusException(false, "Transaction has already started. Please end the transaction.");
        }

        UUID newTxnId = UUID.randomUUID();
        enlistmentMap.put(newTxnId, new CompletableFuture<>());
        transactionIdBinary.set(newTxnId);

        return factory.getController()
                .thenApply(controller -> controller.declareAsync()
                        //.thenAccept(transactionIdBinary::set));
                        //.thenAccept(txnId -> { System.out.println("Adding to TL: " + Thread.currentThread().getId()); transactionIdBinary.set(txnId); }));
                .thenAccept(txnId -> {enlistmentMap.get(newTxnId).complete(txnId);}));
    }

    public static UUID getTransactionId() {
        System.out.println("Reading from TL: " + Thread.currentThread().getId());
        return transactionIdBinary.get();
    }

    public static CompletableFuture<Void> endTransaction(MessagingFactory factory, boolean commit) {
        return factory.getController()
                .thenAcceptAsync(controller -> controller.dischargeAsync(getTransactionId(), commit)
                        .thenRun(transactionIdBinary::remove));
    }
    */
}
