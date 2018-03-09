package com.microsoft.azure.servicebus;

import java.nio.ByteBuffer;

public class TransactionContext {
    public static TransactionContext NULL_TXN = new TransactionContext(null);

    private ByteBuffer txnId;
    private ITransactionHandler txnHandler = null;

    public TransactionContext(ByteBuffer txnId) {
        this.txnId = txnId;
    }

    public ByteBuffer getTransactionId() { return this.txnId; }

    @Override
    public String toString() {
        return new String(txnId.array(), txnId.position(), txnId.limit());
    }

    public void notifyTransactionCompletion(boolean commit) {
        if (txnHandler != null) {
            txnHandler.onTransactionCompleted(commit);
        }
    }

    void registerHandler(ITransactionHandler handler)
    {
        this.txnHandler = handler;
    }

    interface ITransactionHandler {
        public void onTransactionCompleted(boolean commit);
    }
}
