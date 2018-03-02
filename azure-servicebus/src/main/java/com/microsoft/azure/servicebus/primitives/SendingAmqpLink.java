/*package com.microsoft.azure.servicebus.primitives;

import com.microsoft.azure.servicebus.amqp.*;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.engine.impl.DeliveryImpl;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

public class SendingAmqpLink extends ClientEntity implements IAmqpSender, IErrorContextProvider {

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(SendingAmqpLink.class);
    private static final String SEND_TIMED_OUT = "Send operation timed out";
    private static final Duration LINK_REOPEN_TIMEOUT = Duration.ofMinutes(5); // service closes link long before this timeout expires

    private final MessagingFactory underlyingFactory;
    private final String sendPath;
    private final String sasTokenAudienceURI;
    private final Duration operationTimeout;
    private final RetryPolicy retryPolicy;
    private final CompletableFuture<Void> linkClose;
    private final Object pendingSendLock;
    private final ConcurrentHashMap<String, SendWorkItem<Delivery>> pendingSendsData;
    private final PriorityQueue<WeightedDeliveryTag> pendingSends;
    private final DispatchHandler sendWork;
    private boolean isSendLoopRunning;

    private Sender sendLink;
    private CompletableFuture<SendingAmqpLink> linkFirstOpen;
    private int linkCredit;
    private Exception lastKnownLinkError;
    private Instant lastKnownErrorReportedAt;
    private ScheduledFuture<?> sasTokenRenewTimerFuture;
    private CompletableFuture<Void> sendLinkReopenFuture;

    public static CompletableFuture<SendingAmqpLink> create(
            final MessagingFactory factory,
            final String sendLinkName,
            final String senderPath)
    {
        TRACE_LOGGER.info("Creating core message sender to '{}'", senderPath);
        final SendingAmqpLink msgSender = new SendingAmqpLink(factory, sendLinkName, senderPath);
        TimeoutTracker openLinkTracker = TimeoutTracker.create(factory.getOperationTimeout());
        msgSender.initializeLinkOpen(openLinkTracker);

        msgSender.sendTokenAndSetRenewTimer(false).handleAsync((v, sasTokenEx) -> {
            if(sasTokenEx != null)
            {
                Throwable cause = ExceptionUtil.extractAsyncCompletionCause(sasTokenEx);
                TRACE_LOGGER.error("Sending SAS Token to '{}' failed.", msgSender.sendPath, cause);
                msgSender.linkFirstOpen.completeExceptionally(cause);
            }
            else
            {
                try
                {
                    msgSender.underlyingFactory.scheduleOnReactorThread(new DispatchHandler()
                    {
                        @Override
                        public void onEvent()
                        {
                            msgSender.createSendLink();
                        }
                    });
                }
                catch (IOException ioException)
                {
                    msgSender.cancelSASTokenRenewTimer();
                    msgSender.linkFirstOpen.completeExceptionally(new ServiceBusException(false, "Failed to create Sender, see cause for more details.", ioException));
                }
            }

            return null;
        });


        return msgSender.linkFirstOpen;
    }

    private SendingAmqpLink(final MessagingFactory factory, final String sendLinkName, final LinkProperties linkProperties)
    {
        super(sendLinkName, factory);

        this.sendPath = linkProperties.linkPath;
        this.sasTokenAudienceURI = String.format(ClientConstants.SAS_TOKEN_AUDIENCE_FORMAT, factory.getHostName(), this.sendPath);
        this.underlyingFactory = factory;
        this.operationTimeout = factory.getOperationTimeout();

        this.lastKnownLinkError = null;
        this.lastKnownErrorReportedAt = Instant.EPOCH;

        this.retryPolicy = factory.getRetryPolicy();

        this.pendingSendLock = new Object();
        this.pendingSendsData = new ConcurrentHashMap<>();
        this.pendingSends = new PriorityQueue<>(1000, new DeliveryTagComparator());
        this.linkCredit = 0;

        this.linkClose = new CompletableFuture<>();
        this.sendLinkReopenFuture = null;
        this.isSendLoopRunning = false;
        this.sendWork = new DispatchHandler()
        {
            @Override
            public void onEvent()
            {
                SendingAmqpLink.this.processSendWork();
            }
        };
    }

    public String getSendPath()
    {
        return this.sendPath;
    }

    private static String generateRandomDeliveryTag()
    {
        return UUID.randomUUID().toString().replace("-", StringUtil.EMPTY);
    }

    private CompletableFuture<Delivery> sendCoreAsync(
            final byte[] bytes,
            final int arrayOffset,
            final int messageFormat)
    {
        this.throwIfClosed(this.lastKnownLinkError);
        TRACE_LOGGER.debug("Sending message to '{}'", this.sendPath);
        String deliveryTag = SendingAmqpLink.generateRandomDeliveryTag();
        CompletableFuture<Delivery> onSendFuture = new CompletableFuture<>();
        SendWorkItem<Delivery> sendWorkItem = new SendWorkItem<>(bytes, arrayOffset, messageFormat, deliveryTag, onSendFuture, this.operationTimeout);
        this.enlistSendRequest(deliveryTag, sendWorkItem, false);
        this.scheduleSendTimeout(sendWorkItem);
        return onSendFuture;
    }

    private void scheduleSendTimeout(SendWorkItem<Delivery> sendWorkItem)
    {
        // Timer to timeout the request
        ScheduledFuture<?> timeoutTask = Timer.schedule(() -> {
            if (!sendWorkItem.getWork().isDone())
            {
                TRACE_LOGGER.warn("Delivery '{}' to '{}' did not receive ack from service. Throwing timeout.", sendWorkItem.getDeliveryTag(), SendingAmqpLink.this.sendPath);
                SendingAmqpLink.this.pendingSendsData.remove(sendWorkItem.getDeliveryTag());
                SendingAmqpLink.this.throwSenderTimeout(sendWorkItem.getWork(), sendWorkItem.getLastKnownException());
                // Weighted delivery tag not removed from the pending sends queue, but send loop will ignore it anyway if it is present
            }
        },
            sendWorkItem.getTimeoutTracker().remaining(),
            TimerType.OneTimeRun);
        sendWorkItem.setTimeoutTask(timeoutTask);
    }

    private void enlistSendRequest(String deliveryTag, SendWorkItem<Delivery> sendWorkItem, boolean isRetrySend)
    {
        synchronized (this.pendingSendLock)
        {
            this.pendingSendsData.put(deliveryTag, sendWorkItem);
            this.pendingSends.offer(new WeightedDeliveryTag(deliveryTag, isRetrySend ? 1 : 0));

            if(!this.isSendLoopRunning)
            {
                try
                {
                    this.underlyingFactory.scheduleOnReactorThread(this.sendWork);
                }
                catch (IOException ioException)
                {
                    AsyncUtil.completeFutureExceptionally(sendWorkItem.getWork(), new ServiceBusException(false, "Send failed while dispatching to Reactor, see cause for more details.", ioException));
                }
            }
        }
    }

    private void reSendAsync(String deliveryTag, SendWorkItem<Delivery> retryingSendWorkItem, boolean reuseDeliveryTag)
    {
        if(!retryingSendWorkItem.getWork().isDone() && retryingSendWorkItem.cancelTimeoutTask(false))
        {
            Duration remainingTime = retryingSendWorkItem.getTimeoutTracker().remaining();
            if(!remainingTime.isNegative() && !remainingTime.isZero())
            {
                if(!reuseDeliveryTag)
                {
                    deliveryTag = SendingAmqpLink.generateRandomDeliveryTag();
                    retryingSendWorkItem.setDeliveryTag(deliveryTag);
                }

                this.enlistSendRequest(deliveryTag, retryingSendWorkItem, true);
                this.scheduleSendTimeout(retryingSendWorkItem);
            }
        }
    }

    public CompletableFuture<Delivery> sendAsync(final Iterable<Message> messages)
    {
        if (messages == null || IteratorUtil.sizeEquals(messages, 0))
        {
            throw new IllegalArgumentException("Sending Empty batch of messages is not allowed.");
        }

        TRACE_LOGGER.debug("Sending a batch of messages to '{}'", this.sendPath);

        Message firstMessage = messages.iterator().next();
        if (IteratorUtil.sizeEquals(messages, 1))
        {
            return this.sendAsync(firstMessage);
        }

        // proton-j doesn't support multiple dataSections to be part of AmqpMessage
        // here's the alternate approach provided by them: https://github.com/apache/qpid-proton/pull/54
        Message batchMessage = Proton.message();
        batchMessage.setMessageAnnotations(firstMessage.getMessageAnnotations());

        byte[] bytes;
        int byteArrayOffset;
        try
        {
            Pair<byte[], Integer> encodedPair = Util.encodeMessageToMaxSizeArray(batchMessage);
            bytes = encodedPair.getFirstItem();
            byteArrayOffset = encodedPair.getSecondItem();

            for(Message amqpMessage: messages)
            {
                Message messageWrappedByData = Proton.message();
                encodedPair = Util.encodeMessageToOptimalSizeArray(amqpMessage);
                messageWrappedByData.setBody(new Data(new Binary(encodedPair.getFirstItem(), 0, encodedPair.getSecondItem())));

                int encodedSize = Util.encodeMessageToCustomArray(messageWrappedByData, bytes, byteArrayOffset, ClientConstants.MAX_MESSAGE_LENGTH_BYTES - byteArrayOffset - 1);
                byteArrayOffset = byteArrayOffset + encodedSize;
            }
        }
        catch(PayloadSizeExceededException ex)
        {
            TRACE_LOGGER.error("Payload size of batch of messages exceeded limit", ex);
            final CompletableFuture<Delivery> sendTask = new CompletableFuture<>();
            sendTask.completeExceptionally(ex);
            return sendTask;
        }

        return this.sendCoreAsync(bytes, byteArrayOffset, AmqpConstants.AMQP_BATCH_MESSAGE_FORMAT);
    }

    public CompletableFuture<Delivery> sendAsync(Message msg)
    {
        try
        {
            Pair<byte[], Integer> encodedPair = Util.encodeMessageToOptimalSizeArray(msg);
            return this.sendCoreAsync(encodedPair.getFirstItem(), encodedPair.getSecondItem(), DeliveryImpl.DEFAULT_MESSAGE_FORMAT);
        }
        catch(PayloadSizeExceededException exception)
        {
            TRACE_LOGGER.error("Payload size of message exceeded limit", exception);
            final CompletableFuture<Delivery> sendTask = new CompletableFuture<>();
            sendTask.completeExceptionally(exception);
            return sendTask;
        }
    }

    @Override
    public void onOpenComplete(Exception completionException)
    {
        if (completionException == null)
        {
            this.underlyingFactory.registerForConnectionError(this.sendLink);
            this.lastKnownLinkError = null;
            this.retryPolicy.resetRetryCount(this.getClientId());

            if(this.sendLinkReopenFuture != null && !this.sendLinkReopenFuture.isDone())
            {
                AsyncUtil.completeFuture(this.sendLinkReopenFuture, null);
                this.sendLinkReopenFuture = null;
            }

            if (!this.linkFirstOpen.isDone())
            {
                TRACE_LOGGER.info("Opened send link to '{}'", this.sendPath);
                AsyncUtil.completeFuture(this.linkFirstOpen, this);
            }
            else
            {
                synchronized (this.pendingSendLock)
                {
                    if (!this.pendingSendsData.isEmpty())
                    {
                        LinkedList<String> unacknowledgedSends = new LinkedList<>();
                        unacknowledgedSends.addAll(this.pendingSendsData.keySet());

                        if (unacknowledgedSends.size() > 0)
                        {
                            Iterator<String> reverseReader = unacknowledgedSends.iterator();
                            while (reverseReader.hasNext())
                            {
                                String unacknowledgedSend = reverseReader.next();
                                if (this.pendingSendsData.get(unacknowledgedSend).isWaitingForAck())
                                {
                                    this.pendingSends.offer(new WeightedDeliveryTag(unacknowledgedSend, 1));
                                }
                            }
                        }

                        unacknowledgedSends.clear();
                    }
                }
            }
        }
        else
        {
            this.cancelSASTokenRenewTimer();
            if (!this.linkFirstOpen.isDone())
            {
                TRACE_LOGGER.error("Opening send link to '{}' failed", this.sendPath, completionException);
                this.setClosed();
                ExceptionUtil.completeExceptionally(this.linkFirstOpen, completionException, this, true);
            }

            if(this.sendLinkReopenFuture != null && !this.sendLinkReopenFuture.isDone())
            {
                TRACE_LOGGER.warn("Opening send link to '{}' failed", this.sendPath, completionException);
                AsyncUtil.completeFutureExceptionally(this.sendLinkReopenFuture, completionException);
                this.sendLinkReopenFuture = null;
            }
        }
    }

    @Override
    public void onClose(ErrorCondition condition)
    {
        Exception completionException = condition != null ? ExceptionUtil.toException(condition)
                : new ServiceBusException(ClientConstants.DEFAULT_IS_TRANSIENT,
                "The entity has been closed due to transient failures (underlying link closed), please retry the operation.");
        this.onError(completionException);
    }

    @Override
    public void onError(Exception completionException)
    {
        this.linkCredit = 0;
        if (this.getIsClosingOrClosed())
        {
            Exception failureException = completionException == null
                    ? new OperationCancelledException("Send cancelled as the Sender instance is Closed before the sendOperation completed.")
                    : completionException;
            this.clearAllPendingSendsWithException(failureException);

            TRACE_LOGGER.info("Send link to '{}' closed", this.sendPath);
            AsyncUtil.completeFuture(this.linkClose, null);
            return;
        }
        else
        {
            this.underlyingFactory.deregisterForConnectionError(this.sendLink);
            this.lastKnownLinkError = completionException;
            this.lastKnownErrorReportedAt = Instant.now();

            this.onOpenComplete(completionException);

            if (completionException != null &&
                    (!(completionException instanceof ServiceBusException) || !((ServiceBusException) completionException).getIsTransient()))
            {
                TRACE_LOGGER.warn("Send link to '{}' closed. Failing all pending send requests.", this.sendPath);
                this.clearAllPendingSendsWithException(completionException);
            }
            else
            {
                final Map.Entry<String, SendWorkItem<Delivery>> pendingSendEntry = IteratorUtil.getFirst(this.pendingSendsData.entrySet());
                if (pendingSendEntry != null && pendingSendEntry.getValue() != null)
                {
                    final TimeoutTracker tracker = pendingSendEntry.getValue().getTimeoutTracker();
                    if (tracker != null)
                    {
                        final Duration nextRetryInterval = this.retryPolicy.getNextRetryInterval(this.getClientId(), completionException, tracker.remaining());
                        if (nextRetryInterval != null)
                        {
                            TRACE_LOGGER.warn("Send link to '{}' closed. Will retry link creation after '{}'.", this.sendPath, nextRetryInterval);
                            Timer.schedule(() -> {SendingAmqpLink.this.ensureLinkIsOpen();}, nextRetryInterval, TimerType.OneTimeRun);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void onSendComplete(final Delivery delivery)
    {
        final DeliveryState outcome = delivery.getRemoteState();
        final String deliveryTag = new String(delivery.getTag());

        TRACE_LOGGER.debug("Received ack for delivery. path:{}, linkName:{}, deliveryTag:{}, outcome:{}", SendingAmqpLink.this.sendPath, this.sendLink.getName(), deliveryTag, outcome);
        final SendWorkItem<Delivery> pendingSendWorkItem = this.pendingSendsData.remove(deliveryTag);

        if (pendingSendWorkItem != null)
        {
            if (outcome instanceof Accepted)
            {
                this.lastKnownLinkError = null;
                this.retryPolicy.resetRetryCount(this.getClientId());

                pendingSendWorkItem.cancelTimeoutTask(false);
                AsyncUtil.completeFuture(pendingSendWorkItem.getWork(), null);
            }
            else if (outcome instanceof Rejected)
            {
                Rejected rejected = (Rejected) outcome;
                ErrorCondition error = rejected.getError();
                Exception exception = ExceptionUtil.toException(error);

                if (ExceptionUtil.isGeneralError(error.getCondition()))
                {
                    this.lastKnownLinkError = exception;
                    this.lastKnownErrorReportedAt = Instant.now();
                }

                Duration retryInterval = this.retryPolicy.getNextRetryInterval(
                        this.getClientId(), exception, pendingSendWorkItem.getTimeoutTracker().remaining());
                if (retryInterval == null)
                {
                    this.cleanupFailedSend(pendingSendWorkItem, exception);
                }
                else
                {
                    TRACE_LOGGER.warn("Send failed for delivery '{}'. Will retry after '{}'", deliveryTag, retryInterval);
                    pendingSendWorkItem.setLastKnownException(exception);
                    Timer.schedule(() -> {SendingAmqpLink.this.reSendAsync(deliveryTag, pendingSendWorkItem, false);}, retryInterval, TimerType.OneTimeRun);
                }
            }
            else if (outcome instanceof Released)
            {
                this.cleanupFailedSend(pendingSendWorkItem, new OperationCancelledException(outcome.toString()));
            }
            else
            {
                this.cleanupFailedSend(pendingSendWorkItem, new ServiceBusException(false, outcome.toString()));
            }
        }
        else
        {
            TRACE_LOGGER.warn("Delivery mismatch. path:{}, linkName:{}, delivery:{}", this.sendPath, this.sendLink.getName(), deliveryTag);
        }
    }

    private void clearAllPendingSendsWithException(Exception failureException)
    {
        synchronized (this.pendingSendLock)
        {
            for (Map.Entry<String, SendWorkItem<Delivery>> pendingSend: this.pendingSendsData.entrySet())
            {
                this.cleanupFailedSend(pendingSend.getValue(), failureException);
            }

            this.pendingSendsData.clear();
            this.pendingSends.clear();
        }
    }

    private void cleanupFailedSend(final SendWorkItem<Delivery> failedSend, final Exception exception)
    {
        failedSend.cancelTimeoutTask(false);
        ExceptionUtil.completeExceptionally(failedSend.getWork(), exception, this, true);
    }

    private void createSendLink()
    {
        TRACE_LOGGER.info("Creating send link to '{}'", this.sendPath);
        final Connection connection = this.underlyingFactory.getConnection();
        final Session session = connection.session();
        session.setOutgoingWindow(Integer.MAX_VALUE);
        session.open();
        BaseHandler.setHandler(session, new SessionHandler(sendPath));

        final String sendLinkNamePrefix = StringUtil.getShortRandomString();
        final String sendLinkName = !StringUtil.isNullOrEmpty(connection.getRemoteContainer()) ?
                sendLinkNamePrefix.concat(TrackingUtil.TRACKING_ID_TOKEN_SEPARATOR).concat(connection.getRemoteContainer()) :
                sendLinkNamePrefix;

        final Sender sender = session.sender(sendLinkName);
        final Target target = new Target();
        target.setAddress(sendPath);
        sender.setTarget(target);

        final Source source = new Source();
        sender.setSource(source);

        SenderSettleMode settleMode = SenderSettleMode.UNSETTLED;
        TRACE_LOGGER.debug("Send link settle mode '{}'", settleMode);
        sender.setSenderSettleMode(settleMode);

        Map<Symbol, Object> linkProperties = new HashMap<>();
        // ServiceBus expects timeout to be of type unsignedint
        linkProperties.put(ClientConstants.LINK_TIMEOUT_PROPERTY, UnsignedInteger.valueOf(Util.adjustServerTimeout(this.underlyingFactory.getOperationTimeout()).toMillis()));
        sender.setProperties(linkProperties);

        SendLinkHandler handler = new SendLinkHandler(SendingAmqpLink.this);
        BaseHandler.setHandler(sender, handler);
        sender.open();
        this.sendLink = sender;
    }

    CompletableFuture<Void> sendTokenAndSetRenewTimer(boolean retryOnFailure)
    {
        if(this.getIsClosingOrClosed())
        {
            return CompletableFuture.completedFuture(null);
        }
        else
        {
            CompletableFuture<ScheduledFuture<?>> sendTokenFuture = this.underlyingFactory.sendSecurityTokenAndSetRenewTimer(this.sasTokenAudienceURI, retryOnFailure, () -> this.sendTokenAndSetRenewTimer(true));
            return sendTokenFuture.thenAccept((f) -> {this.sasTokenRenewTimerFuture = f; TRACE_LOGGER.debug("Sent SAS Token and set renew timer");});
        }
    }

    private void cancelSASTokenRenewTimer()
    {
        if(this.sasTokenRenewTimerFuture != null && !this.sasTokenRenewTimerFuture.isDone())
        {
            this.sasTokenRenewTimerFuture.cancel(true);
            TRACE_LOGGER.debug("Cancelled SAS Token renew timer");
        }
    }

    // TODO: consolidate common-code written for timeouts in Sender/Receiver
    private void initializeLinkOpen(TimeoutTracker timeout)
    {
        this.linkFirstOpen = new CompletableFuture<>();

        // timer to signal a timeout if exceeds the operationTimeout on MessagingFactory
        Timer.schedule(
                new Runnable()
                {
                    public void run()
                    {
                        if (!SendingAmqpLink.this.linkFirstOpen.isDone())
                        {
                            SendingAmqpLink.this.closeInternals(false);
                            SendingAmqpLink.this.setClosed();

                            Exception operationTimedout = new TimeoutException(
                                    String.format(Locale.US, "Open operation on SendLink(%s) on Entity(%s) timed out at %s.",	SendingAmqpLink.this.sendLink.getName(), SendingAmqpLink.this.getSendPath(), ZonedDateTime.now().toString()),
                                    SendingAmqpLink.this.lastKnownErrorReportedAt.isAfter(Instant.now().minusSeconds(ClientConstants.SERVER_BUSY_BASE_SLEEP_TIME_IN_SECS)) ? SendingAmqpLink.this.lastKnownLinkError : null);
                            TRACE_LOGGER.warn(operationTimedout.getMessage());
                            ExceptionUtil.completeExceptionally(SendingAmqpLink.this.linkFirstOpen, operationTimedout, SendingAmqpLink.this, false);
                        }
                    }
                }
                , timeout.remaining()
                , TimerType.OneTimeRun);
    }

    @Override
    public ErrorContext getContext()
    {
        final boolean isLinkOpened = this.linkFirstOpen != null && this.linkFirstOpen.isDone();
        final String referenceId = this.sendLink != null && this.sendLink.getRemoteProperties() != null && this.sendLink.getRemoteProperties().containsKey(ClientConstants.TRACKING_ID_PROPERTY)
                ? this.sendLink.getRemoteProperties().get(ClientConstants.TRACKING_ID_PROPERTY).toString()
                : ((this.sendLink != null) ? this.sendLink.getName() : null);

        SenderErrorContext errorContext = new SenderErrorContext(
                this.underlyingFactory!=null ? this.underlyingFactory.getHostName() : null,
                this.sendPath,
                referenceId,
                isLinkOpened && this.sendLink != null ? this.sendLink.getCredit() : null);
        return errorContext;
    }

    @Override
    public void onFlow(final int creditIssued)
    {
        this.lastKnownLinkError = null;

        if (creditIssued <= 0)
            return;

        TRACE_LOGGER.debug("Received flow frame. path:{}, linkName:{}, remoteLinkCredit:{}, pendingSendsWaitingForCredit:{}, pendingSendsWaitingDelivery:{}",
                this.sendPath, this.sendLink.getName(), creditIssued, this.pendingSends.size(), this.pendingSendsData.size() - this.pendingSends.size());

        this.linkCredit = this.linkCredit + creditIssued;
        this.sendWork.onEvent();
    }

    private synchronized CompletableFuture<Void> ensureLinkIsOpen()
    {
        // Send SAS token before opening a link as connection might have been closed and reopened
        if (this.sendLink.getLocalState() == EndpointState.CLOSED || this.sendLink.getRemoteState() == EndpointState.CLOSED)
        {
            if(this.sendLinkReopenFuture == null)
            {
                TRACE_LOGGER.info("Recreating send link to '{}'", this.sendPath);
                this.retryPolicy.incrementRetryCount(SendingAmqpLink.this.getClientId());
                this.sendLinkReopenFuture = new CompletableFuture<>();
                // Variable just to closed over by the scheduled runnable. The runnable should cancel only the closed over future, not the parent's instance variable which can change
                final CompletableFuture<Void> linkReopenFutureThatCanBeCancelled = this.sendLinkReopenFuture;
                Timer.schedule(
                        () -> {
                            if (!linkReopenFutureThatCanBeCancelled.isDone())
                            {
                                SendingAmqpLink.this.cancelSASTokenRenewTimer();
                                Exception operationTimedout = new TimeoutException(
                                        String.format(Locale.US, "%s operation on SendLink(%s) to path(%s) timed out at %s.", "Open", SendingAmqpLink.this.sendLink.getName(), SendingAmqpLink.this.sendPath, ZonedDateTime.now()));

                                TRACE_LOGGER.warn(operationTimedout.getMessage());
                                linkReopenFutureThatCanBeCancelled.completeExceptionally(operationTimedout);
                            }
                        }
                        , SendingAmqpLink.LINK_REOPEN_TIMEOUT
                        , TimerType.OneTimeRun);
                this.cancelSASTokenRenewTimer();
                this.sendTokenAndSetRenewTimer(false).handleAsync((v, sendTokenEx) -> {
                    if(sendTokenEx != null)
                    {
                        this.sendLinkReopenFuture.completeExceptionally(sendTokenEx);
                    }
                    else
                    {
                        try
                        {
                            this.underlyingFactory.scheduleOnReactorThread(new DispatchHandler()
                            {
                                @Override
                                public void onEvent()
                                {
                                    SendingAmqpLink.this.createSendLink();
                                }
                            });
                        }
                        catch (IOException ioEx)
                        {
                            this.sendLinkReopenFuture.completeExceptionally(ioEx);
                        }
                    }
                    return null;
                });
            }

            return this.sendLinkReopenFuture;
        }
        else
        {
            return CompletableFuture.completedFuture(null);
        }
    }

    // actual send on the SenderLink should happen only in this method & should run on Reactor Thread
    private void processSendWork()
    {
        synchronized (this.pendingSendLock)
        {
            if(!this.isSendLoopRunning)
            {
                this.isSendLoopRunning = true;
            }
            else
            {
                return;
            }
        }

        TRACE_LOGGER.debug("Processing pending sends to '{}'. Available link credit '{}'", this.sendPath, this.linkCredit);
        try
        {
            if(!this.ensureLinkIsOpen().isDone())
            {
                // Link recreation is pending
                return;
            }

            final Sender sendLinkCurrent = this.sendLink;
            while (sendLinkCurrent != null
                    && sendLinkCurrent.getLocalState() == EndpointState.ACTIVE && sendLinkCurrent.getRemoteState() == EndpointState.ACTIVE
                    && this.linkCredit > 0)
            {
                final WeightedDeliveryTag deliveryTag;
                final SendWorkItem<Delivery> sendData;
                synchronized (this.pendingSendLock)
                {
                    deliveryTag = this.pendingSends.poll();
                    if (deliveryTag == null)
                    {
                        TRACE_LOGGER.debug("There are no pending sends to '{}'.", this.sendPath);
                        // Must be done inside this synchronized block
                        this.isSendLoopRunning = false;
                        break;
                    }
                    else
                    {
                        sendData = this.pendingSendsData.get(deliveryTag.getDeliveryTag());
                        if(sendData == null)
                        {
                            TRACE_LOGGER.warn("SendData not found for this delivery. path:{}, linkName:{}, deliveryTag:{}", this.sendPath, this.sendLink.getName(), deliveryTag);
                            continue;
                        }
                    }
                }

                if (sendData.getWork() != null && sendData.getWork().isDone())
                {
                    // CoreSend could enqueue Sends into PendingSends Queue and can fail the SendCompletableFuture
                    // (when It fails to schedule the ProcessSendWork on reactor Thread)
                    this.pendingSendsData.remove(sendData);
                    continue;
                }

                Delivery delivery = null;
                boolean linkAdvance = false;
                int sentMsgSize = 0;
                Exception sendException = null;

                try
                {
                    delivery = sendLinkCurrent.delivery(deliveryTag.getDeliveryTag().getBytes());
                    delivery.setMessageFormat(sendData.getMessageFormat());
                    TRACE_LOGGER.debug("Sending message delivery '{}' to '{}'", deliveryTag.getDeliveryTag(), this.sendPath);
                    sentMsgSize = sendLinkCurrent.send(sendData.getMessage(), 0, sendData.getEncodedMessageSize());
                    assert sentMsgSize == sendData.getEncodedMessageSize() : "Contract of the ProtonJ library for Sender.Send API changed";

                    linkAdvance = sendLinkCurrent.advance();
                }
                catch(Exception exception)
                {
                    sendException = exception;
                }

                if (linkAdvance)
                {
                    this.linkCredit--;
                    sendData.setWaitingForAck();
                }
                else
                {
                    TRACE_LOGGER.warn("Sendlink advance failed. path:{}, linkName:{}, deliveryTag:{}, sentMessageSize:{}, payloadActualSiz:{}",
                            this.sendPath, this.sendLink.getName(), deliveryTag, sentMsgSize, sendData.getEncodedMessageSize());

                    if (delivery != null)
                    {
                        delivery.free();
                    }

                    Exception completionException = sendException != null ? new OperationCancelledException("Send operation failed. Please see cause for more details", sendException)
                            : new OperationCancelledException(String.format(Locale.US, "Send operation failed while advancing delivery(tag: %s) on SendLink(path: %s).", this.sendPath, deliveryTag));
                    AsyncUtil.completeFutureExceptionally(sendData.getWork(), completionException);
                }
            }
        }
        finally
        {
            synchronized (this.pendingSendLock)
            {
                if(this.isSendLoopRunning)
                {
                    this.isSendLoopRunning = false;
                }
            }
        }
    }

    private void throwSenderTimeout(CompletableFuture<Delivery> pendingSendWork, Exception lastKnownException)
    {
        Exception cause = lastKnownException;
        if (lastKnownException == null && this.lastKnownLinkError != null)
        {
            cause = this.lastKnownErrorReportedAt.isAfter(Instant.now().minusMillis(this.operationTimeout.toMillis())) ? this.lastKnownLinkError	: null;
        }

        boolean isClientSideTimeout = (cause == null || !(cause instanceof ServiceBusException));
        ServiceBusException exception = isClientSideTimeout
                ? new TimeoutException(String.format(Locale.US, "%s %s %s.", SendingAmqpLink.SEND_TIMED_OUT, " at ", ZonedDateTime.now(), cause))
                : (ServiceBusException) cause;

        TRACE_LOGGER.error("Send timed out", exception);
        ExceptionUtil.completeExceptionally(pendingSendWork, exception, this, true);
    }

    private void scheduleLinkCloseTimeout(final TimeoutTracker timeout)
    {
        // timer to signal a timeout if exceeds the operationTimeout on MessagingFactory
        Timer.schedule(
                new Runnable()
                {
                    public void run()
                    {
                        if (!linkClose.isDone())
                        {
                            Exception operationTimedout = new TimeoutException(String.format(Locale.US, "%s operation on Send Link(%s) timed out at %s", "Close", SendingAmqpLink.this.sendLink.getName(), ZonedDateTime.now()));
                            TRACE_LOGGER.warn(operationTimedout.getMessage());

                            ExceptionUtil.completeExceptionally(linkClose, operationTimedout, SendingAmqpLink.this, false);
                        }
                    }
                }
                , timeout.remaining()
                , TimerType.OneTimeRun);
    }

    @Override
    protected CompletableFuture<Void> onClose()
    {
        this.closeInternals(true);
        return this.linkClose;
    }

    private void closeInternals(boolean waitForCloseCompletion)
    {
        if (!this.getIsClosed())
        {
            if (this.sendLink != null && this.sendLink.getLocalState() != EndpointState.CLOSED)
            {
                try {
                    this.underlyingFactory.scheduleOnReactorThread(new DispatchHandler() {

                        @Override
                        public void onEvent() {
                            if (SendingAmqpLink.this.sendLink != null && SendingAmqpLink.this.sendLink.getLocalState() != EndpointState.CLOSED)
                            {
                                TRACE_LOGGER.info("Closing send link to '{}'", SendingAmqpLink.this.sendPath);
                                SendingAmqpLink.this.underlyingFactory.deregisterForConnectionError(SendingAmqpLink.this.sendLink);
                                SendingAmqpLink.this.sendLink.close();
                                if(waitForCloseCompletion)
                                {
                                    SendingAmqpLink.this.scheduleLinkCloseTimeout(TimeoutTracker.create(SendingAmqpLink.this.operationTimeout));
                                }
                                else
                                {
                                    AsyncUtil.completeFuture(SendingAmqpLink.this.linkClose, null);
                                }

                            }
                        }
                    });
                } catch (IOException e) {
                    AsyncUtil.completeFutureExceptionally(this.linkClose, e);
                }
            }
            else
            {
                AsyncUtil.completeFuture(this.linkClose, null);
            }

            this.cancelSASTokenRenewTimer();
        }
    }

    private static class WeightedDeliveryTag
    {
        private final String deliveryTag;
        private final int priority;

        WeightedDeliveryTag(final String deliveryTag, final int priority)
        {
            this.deliveryTag = deliveryTag;
            this.priority = priority;
        }

        public String getDeliveryTag()
        {
            return this.deliveryTag;
        }

        public int getPriority()
        {
            return this.priority;
        }
    }

    private static class DeliveryTagComparator implements Comparator<WeightedDeliveryTag>
    {
        @Override
        public int compare(WeightedDeliveryTag deliveryTag0, WeightedDeliveryTag deliveryTag1)
        {
            return deliveryTag1.getPriority() - deliveryTag0.getPriority();
        }
    }

    public class LinkProperties {
        public String linkPath;
        public Source source;
        public Target target;
        public Map<Symbol, Object> linkProperties;
        public SenderSettleMode settleMode;
    }
}
/*
// Copied from InternalSender of ReqRespLink
class SendingAmqpLink2 extends ClientEntity implements IAmqpSender {
    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(SendingAmqpLink.class);

    private Sender sendLink;
    private Session session;
    private MessagingFactory underlyingFactory;
    private CompletableFuture<Void> openFuture;
    private CompletableFuture<Void> closeFuture;
    private AtomicInteger availableCredit;
    private LinkedList<Message> pendingFreshSends;
    private LinkedList<Message> pendingRetrySends;
    private Object pendingSendsSyncLock;
    private boolean isSendLoopRunning;
    private String linkPath;
    private LinkProperties linkProperties;

    private final Object pendingSendLock;
    private final ConcurrentHashMap<String, SendWorkItem<Delivery>> pendingSendsData;
    private final PriorityQueue<WeightedDeliveryTag> pendingSends;
    private final DispatchHandler sendWork;
    private boolean isSendLoopRunning;

    protected SendingAmqpLink2(String clientId, LinkProperties linkProperties, Session session, MessagingFactory messagingFactory) {
        super(clientId, messagingFactory);
        this.session = session;
        this.linkProperties = linkProperties;
        this.underlyingFactory = messagingFactory;
        this.availableCredit = new AtomicInteger(0);
        this.pendingSendsSyncLock = new Object();
        this.isSendLoopRunning = false;
        this.openFuture = new CompletableFuture<Void>();
        this.closeFuture = new CompletableFuture<Void>();
        this.pendingFreshSends = new LinkedList<>();
        this.pendingRetrySends = new LinkedList<>();

        this.pendingSendLock = new Object();
        this.pendingSendsData = new ConcurrentHashMap<String, SendWorkItem<Delivery>>();
        this.pendingSends = new PriorityQueue<WeightedDeliveryTag>(1000, new DeliveryTagComparator());
        this.linkCredit = 0;
    }

    @Override
    protected CompletableFuture<Void> onClose() {
        this.closeInternals(true);
        return this.closeFuture;
    }

    private static void scheduleLinkCloseTimeout(CompletableFuture<Void> closeFuture, Duration timeout, String linkName)
    {
        Timer.schedule(
                new Runnable()
                {
                    public void run()
                    {
                        if (!closeFuture.isDone())
                        {
                            Exception operationTimedout = new TimeoutException(String.format(Locale.US, "%s operation on Link(%s) timed out at %s", "Close", linkName, ZonedDateTime.now()));
                            TRACE_LOGGER.warn("Closing link timed out", operationTimedout);

                            closeFuture.completeExceptionally(operationTimedout);
                        }
                    }
                }
                , timeout
                , TimerType.OneTimeRun);
    }

    private void completeAllPendingRequestsWithException(Exception exception)
    {
        TRACE_LOGGER.warn("Completing all pending requests with exception in request response link to {}", this.linkPath);
        for(SendWorkItem workItem : this.pendingSendsData.values())
        {
            workItem.getWork().completeExceptionally(exception);
            workItem.cancelTimeoutTask(true);
        }

        this.pendingSendsData.clear();
    }

    void closeInternals(boolean waitForCloseCompletion)
    {
        if (!this.getIsClosed())
        {
            if (this.sendLink != null && this.sendLink.getLocalState() != EndpointState.CLOSED)
            {
                try {
                    this.underlyingFactory.scheduleOnReactorThread(new DispatchHandler() {

                        @Override
                        public void onEvent() {
                            if (SendingAmqpLink.this.sendLink != null && SendingAmqpLink.this.sendLink.getLocalState() != EndpointState.CLOSED)
                            {
                                TRACE_LOGGER.debug("Closing send link to {}", SendingAmqpLink.this.linkPath);
                                SendingAmqpLink.this.sendLink.close();
                                SendingAmqpLink.this.underlyingFactory.deregisterForConnectionError(SendingAmqpLink.this.sendLink);
                                if(waitForCloseCompletion)
                                {
                                    SendingAmqpLink.scheduleLinkCloseTimeout(SendingAmqpLink.this.closeFuture, SendingAmqpLink.this.underlyingFactory.getOperationTimeout(), SendingAmqpLink.this.sendLink.getName());
                                }
                                else
                                {
                                    SendingAmqpLink.this.closeFuture.complete(null);
                                }
                            }
                        }
                    });
                } catch (IOException e) {
                    this.closeFuture.completeExceptionally(e);
                }
            }
            else
            {
                this.closeFuture.complete(null);
            }
        }
    }

    @Override
    public void onOpenComplete(Exception completionException) {
        if(completionException == null)
        {
            TRACE_LOGGER.debug("Opened send link to {}", this.linkPath);
            this.underlyingFactory.registerForConnectionError(this.sendLink);
            this.openFuture.complete(null);
            this.runSendLoop();
        }
        else
        {
            TRACE_LOGGER.error("Opening send link to {} failed.", this.linkPath, completionException);
            this.setClosed();
            this.closeFuture.complete(null);
            this.openFuture.completeExceptionally(completionException);
        }
    }

    @Override
    public void onError(Exception exception) {
        if(!this.openFuture.isDone())
        {
            this.onOpenComplete(exception);
        }

        if(this.getIsClosingOrClosed())
        {
            if(!this.closeFuture.isDone())
            {
                TRACE_LOGGER.error("Closing internal send link of requestresponselink to {} failed.", this.linkPath, exception);
                this.closeFuture.completeExceptionally(exception);
            }
        }

        TRACE_LOGGER.warn("Internal send link of requestresponselink to '{}' encountered error.", this.linkPath, exception);
        this.underlyingFactory.deregisterForConnectionError(this.sendLink);
        this.completeAllPendingRequestsWithException(exception);
    }

    @Override
    public void onClose(ErrorCondition condition) {
        if(this.getIsClosingOrClosed())
        {
            if(!this.closeFuture.isDone())
            {
                if(condition == null || condition.getCondition() == null)
                {
                    TRACE_LOGGER.info("Closed send link to {}", this.linkPath);
                    this.closeFuture.complete(null);
                }
                else
                {
                    Exception exception = ExceptionUtil.toException(condition);
                    TRACE_LOGGER.error("Closing send link to {} failed.", this.linkPath, exception);
                    this.closeFuture.completeExceptionally(exception);
                }
            }
        }
        else
        {
            if(condition != null)
            {
                Exception exception = ExceptionUtil.toException(condition);
                if(!this.openFuture.isDone())
                {
                    this.onOpenComplete(exception);
                }
                else
                {
                    TRACE_LOGGER.warn("Send link to '{}' closed with error.", this.linkPath, exception);
                    this.underlyingFactory.deregisterForConnectionError(this.sendLink);
                    this.completeAllPendingRequestsWithException(exception);
                }
            }
        }
    }

    public void sendRequest(Message requestMessage, boolean isRetry)
    {
        synchronized(this.pendingSendsSyncLock)
        {
            if(isRetry)
            {
                this.pendingRetrySends.add(requestMessage);
            }
            else
            {
                this.pendingFreshSends.add(requestMessage);
            }

            // This check must be done inside lock
            if(this.isSendLoopRunning)
            {
                return;
            }
        }

        try {
            this.parent.underlyingFactory.scheduleOnReactorThread(new DispatchHandler() {
                @Override
                public void onEvent() {
                    SendingAmqpLink.this.runSendLoop();
                }
            });
        } catch (IOException e) {
            this.parent.exceptionallyCompleteRequest((String)requestMessage.getMessageId(), e, true);
        }
    }

    public void removeEnqueuedRequest(Message requestMessage, boolean isRetry)
    {
        synchronized(this.pendingSendsSyncLock)
        {
            // Collections are more likely to be very small. So remove() shouldn't be a problem.
            if(isRetry)
            {
                this.pendingRetrySends.remove(requestMessage);
            }
            else
            {
                this.pendingFreshSends.remove(requestMessage);
            }
        }
    }

    @Override
    public void onFlow(int creditIssued) {
        TRACE_LOGGER.debug("RequestResonseLink {} internal sender received credit :{}", this.parent.linkPath, creditIssued);
        this.availableCredit.addAndGet(creditIssued);
        TRACE_LOGGER.debug("RequestResonseLink {} internal sender available credit :{}", this.parent.linkPath, this.availableCredit.get());
        this.runSendLoop();
    }

    @Override
    public void onSendComplete(Delivery delivery) {
        // Doesn't happen as sends are settled on send
    }

    public void setSendLink(Sender sendLink) {
        this.sendLink = sendLink;
        this.availableCredit = new AtomicInteger(0);
    }

    private void runSendLoop()
    {
        synchronized(this.pendingSendsSyncLock)
        {
            if(this.isSendLoopRunning)
            {
                return;
            }
            else
            {
                this.isSendLoopRunning = true;
            }
        }

        TRACE_LOGGER.debug("Starting requestResponseLink {} internal sender send loop", this.parent.linkPath);

        try
        {
            while(this.sendLink != null && this.sendLink.getLocalState() == EndpointState.ACTIVE && this.sendLink.getRemoteState() == EndpointState.ACTIVE && this.availableCredit.get() > 0)
            {
                Message requestToBeSent = null;
                synchronized(pendingSendsSyncLock)
                {
                    // First send retries and then fresh ones
                    requestToBeSent = this.pendingRetrySends.poll();
                    if(requestToBeSent == null)
                    {
                        requestToBeSent = this.pendingFreshSends.poll();
                        if(requestToBeSent == null)
                        {
                            // Set to false inside the synchronized block to avoid race condition
                            this.isSendLoopRunning = false;
                            TRACE_LOGGER.debug("RequestResponseLink {} internal sender send loop ending as there are no more requests enqueued.", this.parent.linkPath);
                            break;
                        }
                    }
                }

                Delivery delivery = this.sendLink.delivery(UUID.randomUUID().toString().getBytes());
                delivery.setMessageFormat(DeliveryImpl.DEFAULT_MESSAGE_FORMAT);

                Pair<byte[], Integer> encodedPair = null;
                try
                {
                    encodedPair = Util.encodeMessageToOptimalSizeArray(requestToBeSent);
                }
                catch(PayloadSizeExceededException exception)
                {
                    this.parent.exceptionallyCompleteRequest((String)requestToBeSent.getMessageId(), new PayloadSizeExceededException(String.format("Size of the payload exceeded Maximum message size: %s kb", ClientConstants.MAX_MESSAGE_LENGTH_BYTES / 1024), exception), false);
                }

                try
                {
                    int sentMsgSize = this.sendLink.send(encodedPair.getFirstItem(), 0, encodedPair.getSecondItem());
                    assert sentMsgSize == encodedPair.getSecondItem() : "Contract of the ProtonJ library for Sender.Send API changed";
                    delivery.settle();
                    this.availableCredit.decrementAndGet();
                    TRACE_LOGGER.debug("RequestResonseLink {} internal sender sent a request. available credit :{}", this.parent.linkPath, this.availableCredit.get());
                }
                catch(Exception e)
                {
                    TRACE_LOGGER.error("RequestResonseLink {} failed to send request with request id:{}.", this.parent.linkPath, requestToBeSent.getMessageId(), e);
                    this.parent.exceptionallyCompleteRequest((String)requestToBeSent.getMessageId(), e, false);
                }
            }
        }
        finally
        {
            synchronized (this.pendingSendsSyncLock) {
                if(this.isSendLoopRunning)
                {
                    this.isSendLoopRunning = false;
                }
            }

            TRACE_LOGGER.debug("RequestResponseLink {} internal sender send loop stopped.", this.parent.linkPath);
        }
    }

    private static class WeightedDeliveryTag
    {
        private final String deliveryTag;
        private final int priority;

        WeightedDeliveryTag(final String deliveryTag, final int priority)
        {
            this.deliveryTag = deliveryTag;
            this.priority = priority;
        }

        public String getDeliveryTag()
        {
            return this.deliveryTag;
        }

        public int getPriority()
        {
            return this.priority;
        }
    }

    private static class DeliveryTagComparator implements Comparator<WeightedDeliveryTag>
    {
        @Override
        public int compare(WeightedDeliveryTag deliveryTag0, WeightedDeliveryTag deliveryTag1)
        {
            return deliveryTag1.getPriority() - deliveryTag0.getPriority();
        }
    }
}
*/