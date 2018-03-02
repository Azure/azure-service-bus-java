package com.microsoft.azure.servicebus.security;

import java.nio.ByteBuffer;

public class TransactionContext {
    public static TransactionContext NULL_TXN = new TransactionContext(null);

    private ByteBuffer txnId;

    public TransactionContext(ByteBuffer txnId) {
        this.txnId = txnId;
    }

    ByteBuffer getTransactionId() { return this.txnId; }
}
