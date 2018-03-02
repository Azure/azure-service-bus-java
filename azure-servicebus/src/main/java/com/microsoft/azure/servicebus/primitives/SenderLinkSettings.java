package com.microsoft.azure.servicebus.primitives;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.Target;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;

import java.util.Map;

// todo: add get set
public class SenderLinkSettings {
    public String linkName;
    public String linkPath;
    public Source source;
    public Target target;
    public Map<Symbol, Object> linkProperties;
    public SenderSettleMode settleMode;
    public boolean requiresAuthentication;
}