package com.microsoft.azure.servicebus;

import com.microsoft.azure.servicebus.primitives.MessagingFactory;

import java.nio.ByteBuffer;

/**
 * Represents an active servicebus transaction.
 * A transaction is initiated by calling {@link MessagingFactory#startTransactionAsync()}.
 * A transaction can result in commit or rollback.
 * To commit, call {@link MessagingFactory#endTransactionAsync(TransactionContext, boolean)} with <code>commit = true</code>
 * To rollback, call {@link MessagingFactory#endTransactionAsync(TransactionContext, boolean)} with <code>commit = false</code>
 */
public class TransactionContext {
    public static TransactionContext NULL_TXN = new TransactionContext(null);

    private ByteBuffer txnId;
    private ITransactionHandler txnHandler = null;

    public TransactionContext(ByteBuffer txnId) {
        this.txnId = txnId;
    }

    /**
     * Represents the service-side transactionID
     * @return transaction ID
     */
    public ByteBuffer getTransactionId() { return this.txnId; }

    @Override
    public String toString() {
        return new String(txnId.array(), txnId.position(), txnId.limit());
    }

    /**
     * This is not to be called by the user.
     */
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
