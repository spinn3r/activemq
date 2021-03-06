/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.amqp;

import static org.apache.activemq.transport.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.apache.activemq.transport.amqp.AmqpSupport.CONNECTION_OPEN_FAILED;
import static org.apache.activemq.transport.amqp.AmqpSupport.COPY;
import static org.apache.activemq.transport.amqp.AmqpSupport.JMS_SELECTOR_FILTER_IDS;
import static org.apache.activemq.transport.amqp.AmqpSupport.NO_LOCAL_FILTER_IDS;
import static org.apache.activemq.transport.amqp.AmqpSupport.QUEUE_PREFIX;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_QUEUE_CAPABILITY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_TOPIC_CAPABILITY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TOPIC_PREFIX;
import static org.apache.activemq.transport.amqp.AmqpSupport.contains;
import static org.apache.activemq.transport.amqp.AmqpSupport.findFilter;
import static org.apache.activemq.transport.amqp.AmqpSupport.toBytes;
import static org.apache.activemq.transport.amqp.AmqpSupport.toLong;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Destination;
import javax.jms.InvalidClientIDException;
import javax.jms.InvalidSelectorException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.security.AuthenticationBroker;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.store.PersistenceAdapterSupport;
import org.apache.activemq.transport.amqp.message.AMQPNativeInboundTransformer;
import org.apache.activemq.transport.amqp.message.AMQPRawInboundTransformer;
import org.apache.activemq.transport.amqp.message.ActiveMQJMSVendor;
import org.apache.activemq.transport.amqp.message.AutoOutboundTransformer;
import org.apache.activemq.transport.amqp.message.EncodedMessage;
import org.apache.activemq.transport.amqp.message.InboundTransformer;
import org.apache.activemq.transport.amqp.message.JMSMappingInboundTransformer;
import org.apache.activemq.transport.amqp.message.OutboundTransformer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.CollectorImpl;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.apache.qpid.proton.message.Message;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpProtocolConverter implements IAmqpProtocolConverter {

    private static final Logger TRACE_FRAMES = AmqpTransportFilter.TRACE_FRAMES;
    private static final Logger LOG = LoggerFactory.getLogger(AmqpProtocolConverter.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};
    private static final int CHANNEL_MAX = 32767;

    private final AmqpTransport amqpTransport;
    private final AmqpWireFormat amqpWireFormat;
    private final BrokerService brokerService;
    private AuthenticationBroker authenticator;

    protected int producerCredit;
    protected Transport protonTransport = Proton.transport();
    protected Connection protonConnection = Proton.connection();
    protected Collector eventCollector = new CollectorImpl();

    public AmqpProtocolConverter(AmqpTransport transport, BrokerService brokerService) {
        this.amqpTransport = transport;
        AmqpInactivityMonitor monitor = transport.getInactivityMonitor();
        if (monitor != null) {
            monitor.setProtocolConverter(this);
        }
        this.amqpWireFormat = transport.getWireFormat();
        this.brokerService = brokerService;

        // the configured maxFrameSize on the URI.
        int maxFrameSize = transport.getWireFormat().getMaxAmqpFrameSize();
        if (maxFrameSize > AmqpWireFormat.NO_AMQP_MAX_FRAME_SIZE) {
            this.protonTransport.setMaxFrameSize(maxFrameSize);
        }

        this.protonTransport.bind(this.protonConnection);

        // NOTE: QPid JMS client has a bug where the channel max is stored as a
        //       short value in the Connection class which means that if we allow
        //       the default channel max of 65535 to be sent then no new sessions
        //       can be created because the value would be -1 when checked.
        this.protonTransport.setChannelMax(CHANNEL_MAX);

        this.protonConnection.collect(eventCollector);

        updateTracer();
    }

    /**
     * Load and return a <code>[]Symbol</code> that contains the connection capabilities
     * offered to new connections
     *
     * @return the capabilities that are offered to new clients on connect.
     */
    protected Symbol[] getConnectionCapabilitiesOffered() {
        return new Symbol[]{ ANONYMOUS_RELAY };
    }

    /**
     * Load and return a <code>Map<Symbol, Object></code> that contains the properties
     * that this connection supplies to incoming connections.
     *
     * @return the properties that are offered to the incoming connection.
     */
    protected Map<Symbol, Object> getConnetionProperties() {
        Map<Symbol, Object> properties = new HashMap<Symbol, Object>();

        properties.put(QUEUE_PREFIX, "queue://");
        properties.put(TOPIC_PREFIX, "topic://");

        return properties;
    }

    /**
     * Load and return a <code>Map<Symbol, Object></code> that contains the properties
     * that this connection supplies to incoming connections when the open has failed
     * and the remote should expect a close to follow.
     *
     * @return the properties that are offered to the incoming connection.
     */
    protected Map<Symbol, Object> getFailedConnetionProperties() {
        Map<Symbol, Object> properties = new HashMap<Symbol, Object>();

        properties.put(CONNECTION_OPEN_FAILED, true);

        return properties;
    }

    @Override
    public void updateTracer() {
        if (amqpTransport.isTrace()) {
            ((TransportImpl) protonTransport).setProtocolTracer(new ProtocolTracer() {
                @Override
                public void receivedFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("{} | RECV: {}", AmqpProtocolConverter.this.amqpTransport.getRemoteAddress(), transportFrame.getBody());
                }

                @Override
                public void sentFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("{} | SENT: {}", AmqpProtocolConverter.this.amqpTransport.getRemoteAddress(), transportFrame.getBody());
                }
            });
        }
    }

    void pumpProtonToSocket() {
        try {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    LOG.trace("Sending {} bytes out", toWrite.limit());
                    amqpTransport.sendToAmqp(toWrite);
                    protonTransport.outputConsumed();
                } else {
                    done = true;
                }
            }
        } catch (IOException e) {
            amqpTransport.onException(e);
        }
    }

    static class AmqpSessionContext {
        private final SessionId sessionId;
        long nextProducerId = 0;
        long nextConsumerId = 0;

        final Map<ConsumerId, ConsumerContext> consumers = new HashMap<ConsumerId, ConsumerContext>();

        public AmqpSessionContext(ConnectionId connectionId, long id) {
            sessionId = new SessionId(connectionId, id);
        }
    }

    Sasl sasl;

    /**
     * Convert a AMQP command
     */
    @Override
    public void onAMQPData(Object command) throws Exception {
        Buffer frame;
        if (command.getClass() == AmqpHeader.class) {
            AmqpHeader header = (AmqpHeader) command;

            if (amqpWireFormat.isHeaderValid(header)) {
                LOG.trace("Connection from an AMQP v1.0 client initiated. {}", header);
            } else {
                LOG.warn("Connection attempt from non AMQP v1.0 client. {}", header);
                AmqpHeader reply = amqpWireFormat.getMinimallySupportedHeader();
                amqpTransport.sendToAmqp(reply.getBuffer());
                handleException(new AmqpProtocolException(
                    "Connection from client using unsupported AMQP attempted", true));
            }

            switch (header.getProtocolId()) {
                case 0:
                    break; // nothing to do..
                case 3: // Client will be using SASL for auth..
                    sasl = protonTransport.sasl();
                    sasl.setMechanisms(new String[] { "ANONYMOUS", "PLAIN" });
                    sasl.server();
                    break;
                default:
            }
            frame = header.getBuffer();
        } else {
            frame = (Buffer) command;
        }
        onFrame(frame);
    }

    public void onFrame(Buffer frame) throws Exception {
        while (frame.length > 0) {
            try {
                int count = protonTransport.input(frame.data, frame.offset, frame.length);
                frame.moveHead(count);
            } catch (Throwable e) {
                handleException(new AmqpProtocolException("Could not decode AMQP frame: " + frame, true, e));
                return;
            }

            try {
                if (sasl != null) {
                    // Lets try to complete the sasl handshake.
                    if (sasl.getRemoteMechanisms().length > 0) {
                        if ("PLAIN".equals(sasl.getRemoteMechanisms()[0])) {
                            byte[] data = new byte[sasl.pending()];
                            sasl.recv(data, 0, data.length);
                            Buffer[] parts = new Buffer(data).split((byte) 0);
                            if (parts.length > 0) {
                                connectionInfo.setUserName(parts[0].utf8().toString());
                            }
                            if (parts.length > 1) {
                                connectionInfo.setPassword(parts[1].utf8().toString());
                            }

                            if (tryAuthenticate(connectionInfo, amqpTransport.getPeerCertificates())) {
                                sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                            } else {
                                sasl.done(Sasl.SaslOutcome.PN_SASL_AUTH);
                            }

                            amqpTransport.getWireFormat().resetMagicRead();
                            sasl = null;
                            LOG.debug("SASL [PLAIN] Handshake complete.");
                        } else if ("ANONYMOUS".equals(sasl.getRemoteMechanisms()[0])) {
                            if (tryAuthenticate(connectionInfo, amqpTransport.getPeerCertificates())) {
                                sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                            } else {
                                sasl.done(Sasl.SaslOutcome.PN_SASL_AUTH);
                            }
                            amqpTransport.getWireFormat().resetMagicRead();
                            sasl = null;
                            LOG.debug("SASL [ANONYMOUS] Handshake complete.");
                        }
                    }
                }

                Event event = null;
                while ((event = eventCollector.peek()) != null) {
                    if (amqpTransport.isTrace()) {
                        LOG.trace("Processing event: {}", event.getType());
                    }
                    switch (event.getType()) {
                        case CONNECTION_REMOTE_OPEN:
                        case CONNECTION_REMOTE_CLOSE:
                            processConnectionEvent(event.getConnection());
                            break;
                        case SESSION_REMOTE_OPEN:
                        case SESSION_REMOTE_CLOSE:
                            processSessionEvent(event.getSession());
                            break;
                        case LINK_REMOTE_OPEN:
                            processLinkOpen(event.getLink());
                            break;
                        case LINK_REMOTE_DETACH:
                            processLinkDetach(event.getLink());
                            break;
                        case LINK_REMOTE_CLOSE:
                            processLinkClose(event.getLink());
                            break;
                        case LINK_FLOW:
                            processLinkFlow(event.getLink());
                            break;
                        case DELIVERY:
                            processDelivery(event.getDelivery());
                            break;
                        default:
                            break;
                    }

                    eventCollector.pop();
                }

            } catch (Throwable e) {
                handleException(new AmqpProtocolException("Could not process AMQP commands", true, e));
            }

            pumpProtonToSocket();
        }
    }

    protected void processLinkFlow(Link link) throws Exception {
        Object context = link.getContext();
        int credit = link.getCredit();
        if (context instanceof ConsumerContext) {
            ConsumerContext consumerContext = (ConsumerContext)context;

            if (credit != consumerContext.credit) {
                consumerContext.credit = credit >= 0 ? credit : 0;
                ConsumerControl control = new ConsumerControl();
                control.setConsumerId(consumerContext.consumerId);
                control.setDestination(consumerContext.destination);
                control.setPrefetch(consumerContext.credit);
                sendToActiveMQ(control, null);
            }
        }
        ((AmqpDeliveryListener) link.getContext()).drainCheck();
    }

    protected void processConnectionEvent(Connection connection) throws Exception {
        EndpointState remoteState = connection.getRemoteState();
        if (remoteState == EndpointState.ACTIVE) {
            onConnectionOpen();
        } else if (remoteState == EndpointState.CLOSED) {
            doClose();
        }
    }

    protected void processLinkOpen(Link link) throws Exception {
        onLinkOpen(link);
    }

    protected void processLinkDetach(Link link) throws Exception {
        AmqpDeliveryListener context = (AmqpDeliveryListener) link.getContext();
        if (context != null) {
            context.onDetach();
        }
        link.detach();
        link.free();
    }

    protected void processLinkClose(Link link) throws Exception {
        AmqpDeliveryListener context = (AmqpDeliveryListener) link.getContext();
        if (context != null) {
            context.onClose();
        }
        link.close();
        link.free();
    }

    protected void processSessionEvent(Session session) throws Exception {
        EndpointState remoteState = session.getRemoteState();
        if (remoteState == EndpointState.ACTIVE) {
            onSessionOpen(session);
        } else if (remoteState == EndpointState.CLOSED) {
            // TODO - close links?
            onSessionClose(session);
        }
    }

    protected void processDelivery(Delivery delivery) throws Exception {
        if (!delivery.isPartial()) {
            AmqpDeliveryListener listener = (AmqpDeliveryListener) delivery.getLink().getContext();
            if (listener != null) {
                listener.onDelivery(delivery);
            }
        }
    }

    boolean closing = false;
    boolean closedSocket = false;

    private void doClose() {
        if (!closing) {
            closing = true;
            sendToActiveMQ(new RemoveInfo(connectionId), new ResponseHandler() {
                @Override
                public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                    protonConnection.close();
                    if (!closedSocket) {
                        pumpProtonToSocket();
                    }
                }
            });
            sendToActiveMQ(new ShutdownInfo(), null);
        }
    }

    @Override
    public void onAMQPException(IOException error) {
        closedSocket = true;
        if (!closing) {
            amqpTransport.sendToActiveMQ(error);
        } else {
            try {
                amqpTransport.stop();
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public void onActiveMQCommand(Command command) throws Exception {
        if (command.isResponse()) {
            Response response = (Response) command;
            ResponseHandler rh = resposeHandlers.remove(Integer.valueOf(response.getCorrelationId()));
            if (rh != null) {
                rh.onResponse(this, response);
            } else {
                // Pass down any unexpected errors. Should this close the connection?
                if (response.isException()) {
                    Throwable exception = ((ExceptionResponse) response).getException();
                    handleException(exception);
                }
            }
        } else if (command.isMessageDispatch()) {
            MessageDispatch md = (MessageDispatch) command;
            ConsumerContext consumerContext = subscriptionsByConsumerId.get(md.getConsumerId());
            if (consumerContext != null) {
                // End of Queue Browse will have no Message object.
                if (md.getMessage() != null) {
                    LOG.trace("Dispatching MessageId: {} to consumer", md.getMessage().getMessageId());
                } else {
                    LOG.trace("Dispatching End of Browse Command to consumer {}", md.getConsumerId());
                }
                consumerContext.onMessageDispatch(md);
                if (md.getMessage() != null) {
                    LOG.trace("Finished Dispatch of MessageId: {} to consumer", md.getMessage().getMessageId());
                }
            }
        } else if (command.getDataStructureType() == ConnectionError.DATA_STRUCTURE_TYPE) {
            // Pass down any unexpected async errors. Should this close the connection?
            Throwable exception = ((ConnectionError) command).getException();
            handleException(exception);
        } else if (command.isBrokerInfo()) {
            // ignore
        } else {
            LOG.debug("Do not know how to process ActiveMQ Command {}", command);
        }
    }

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();
    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final ConnectionInfo connectionInfo = new ConnectionInfo();
    private long nextSessionId = 0;
    private long nextTempDestinationId = 0;

    static abstract class AmqpDeliveryListener {

        protected ActiveMQDestination destination;
        protected List<Runnable> closeActions = new ArrayList<Runnable>();

        abstract public void onDelivery(Delivery delivery) throws Exception;

        public void onDetach() throws Exception {
        }

        public void onClose() throws Exception {

            for (Runnable action : closeActions) {
                action.run();
            }

            closeActions.clear();
        }

        public void drainCheck() {
        }

        abstract void doCommit() throws Exception;

        abstract void doRollback() throws Exception;

        public void addCloseAction(Runnable action) {
            closeActions.add(action);
        }

        public ActiveMQDestination getDestination() {
            return destination;
        }

        public void setDestination(ActiveMQDestination destination) {
            this.destination = destination;
        }
    }

    private void onConnectionOpen() throws AmqpProtocolException {

        connectionInfo.setResponseRequired(true);
        connectionInfo.setConnectionId(connectionId);

        configureInactivityMonitor();

        String clientId = protonConnection.getRemoteContainer();
        if (clientId != null && !clientId.isEmpty()) {
            connectionInfo.setClientId(clientId);
        }

        connectionInfo.setTransportContext(amqpTransport.getPeerCertificates());

        sendToActiveMQ(connectionInfo, new ResponseHandler() {
            @Override
            public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                Throwable exception = null;
                try {
                    if (response.isException()) {
                        protonConnection.setProperties(getFailedConnetionProperties());
                        protonConnection.open();

                        exception = ((ExceptionResponse) response).getException();
                        if (exception instanceof SecurityException) {
                            protonConnection.setCondition(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, exception.getMessage()));
                        } else if (exception instanceof InvalidClientIDException) {
                            protonConnection.setCondition(new ErrorCondition(AmqpError.INVALID_FIELD, exception.getMessage()));
                        } else {
                            protonConnection.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, exception.getMessage()));
                        }

                        protonConnection.close();
                    } else {
                        protonConnection.setOfferedCapabilities(getConnectionCapabilitiesOffered());
                        protonConnection.setProperties(getConnetionProperties());
                        protonConnection.open();
                    }
                } finally {
                    pumpProtonToSocket();

                    if (response.isException()) {
                        amqpTransport.onException(IOExceptionSupport.create(exception));
                    }
                }
            }
        });
    }

    private void onSessionOpen(Session session) {
        AmqpSessionContext sessionContext = new AmqpSessionContext(connectionId, nextSessionId++);
        session.setContext(sessionContext);
        sendToActiveMQ(new SessionInfo(sessionContext.sessionId), null);
        session.setIncomingCapacity(Integer.MAX_VALUE);
        session.open();
    }

    private void onSessionClose(Session session) {
        AmqpSessionContext sessionContext = (AmqpSessionContext) session.getContext();
        if (sessionContext != null) {
            LOG.trace("Session {} closed", sessionContext.sessionId);
            sendToActiveMQ(new RemoveInfo(sessionContext.sessionId), null);
            session.setContext(null);
        }
        session.close();
        session.free();
    }

    private void onLinkOpen(Link link) {
        link.setSource(link.getRemoteSource());
        link.setTarget(link.getRemoteTarget());

        AmqpSessionContext sessionContext = (AmqpSessionContext) link.getSession().getContext();
        if (link instanceof Receiver) {
            onReceiverOpen((Receiver) link, sessionContext);
        } else {
            onSenderOpen((Sender) link, sessionContext);
        }
    }

    private void configureInactivityMonitor() {
        AmqpInactivityMonitor monitor = amqpTransport.getInactivityMonitor();
        if (monitor == null) {
            return;
        }

        monitor.stopConnectChecker();
    }

    InboundTransformer inboundTransformer;

    protected InboundTransformer getInboundTransformer() {
        if (inboundTransformer == null) {
            String transformer = amqpTransport.getTransformer();
            if (transformer.equals(InboundTransformer.TRANSFORMER_JMS)) {
                inboundTransformer = new JMSMappingInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            } else if (transformer.equals(InboundTransformer.TRANSFORMER_NATIVE)) {
                inboundTransformer = new AMQPNativeInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            } else if (transformer.equals(InboundTransformer.TRANSFORMER_RAW)) {
                inboundTransformer = new AMQPRawInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            } else {
                LOG.warn("Unknown transformer type {} using native one instead", transformer);
                inboundTransformer = new AMQPNativeInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            }
        }
        return inboundTransformer;
    }

    abstract class BaseProducerContext extends AmqpDeliveryListener {

        ByteArrayOutputStream current = new ByteArrayOutputStream();

        private final byte[] recvBuffer = new byte[1024 * 8];

        @Override
        public void onDelivery(Delivery delivery) throws Exception {
            Receiver receiver = ((Receiver) delivery.getLink());
            if (!delivery.isReadable()) {
                LOG.debug("Delivery was not readable!");
                return;
            }

            if (current == null) {
                current = new ByteArrayOutputStream();
            }

            int count;
            while ((count = receiver.recv(recvBuffer, 0, recvBuffer.length)) > 0) {
                current.write(recvBuffer, 0, count);
            }

            // Expecting more deliveries..
            if (count == 0) {
                return;
            }

            receiver.advance();
            Buffer buffer = current.toBuffer();
            current = null;
            onMessage(receiver, delivery, buffer);
        }

        @Override
        void doCommit() throws Exception {
        }

        @Override
        void doRollback() throws Exception {
        }

        abstract protected void onMessage(Receiver receiver, Delivery delivery, Buffer buffer) throws Exception;
    }

    class ProducerContext extends BaseProducerContext {
        private final ProducerId producerId;
        private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
        private boolean closed;
        private boolean anonymous;

        public ProducerContext(ProducerId producerId) {
            this.producerId = producerId;
        }

        @Override
        public String toString() {
            return "ProducerContext { producerId = " + producerId + ", destination = " + destination + " }";
        }

        @Override
        protected void onMessage(final Receiver receiver, final Delivery delivery, Buffer buffer) throws Exception {
            if (!closed) {
                EncodedMessage em = new EncodedMessage(delivery.getMessageFormat(), buffer.data, buffer.offset, buffer.length);
                final ActiveMQMessage message = (ActiveMQMessage) getInboundTransformer().transform(em);
                current = null;

                if (destination != null) {
                    message.setJMSDestination(destination);
                } else if (isAnonymous()) {
                    Destination toDestination = message.getJMSDestination();
                    if (toDestination == null || !(toDestination instanceof ActiveMQDestination)) {
                        Rejected rejected = new Rejected();
                        ErrorCondition condition = new ErrorCondition();
                        condition.setCondition(Symbol.valueOf("failed"));
                        condition.setDescription("Missing to field for message sent to an anonymous producer");
                        rejected.setError(condition);
                        delivery.disposition(rejected);
                        return;
                    }
                }
                message.setProducerId(producerId);

                // Always override the AMQP client's MessageId with our own.  Preserve
                // the original in the TextView property for later Ack.
                MessageId messageId = new MessageId(producerId, messageIdGenerator.getNextSequenceId());

                MessageId amqpMessageId = message.getMessageId();
                if (amqpMessageId != null) {
                    if (amqpMessageId.getTextView() != null) {
                        messageId.setTextView(amqpMessageId.getTextView());
                    } else {
                        messageId.setTextView(amqpMessageId.toString());
                    }
                }

                message.setMessageId(messageId);

                LOG.trace("Inbound Message:{} from Producer:{}", message.getMessageId(), producerId + ":" + messageId.getProducerSequenceId());

                final DeliveryState remoteState = delivery.getRemoteState();
                if (remoteState != null && remoteState instanceof TransactionalState) {
                    TransactionalState s = (TransactionalState) remoteState;
                    long txid = toLong(s.getTxnId());
                    message.setTransactionId(new LocalTransactionId(connectionId, txid));
                }

                message.onSend();
                if (!delivery.remotelySettled()) {
                    sendToActiveMQ(message, new ResponseHandler() {

                        @Override
                        public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                            if (response.isException()) {
                                ExceptionResponse er = (ExceptionResponse) response;
                                Rejected rejected = new Rejected();
                                ErrorCondition condition = new ErrorCondition();
                                condition.setCondition(Symbol.valueOf("failed"));
                                condition.setDescription(er.getException().getMessage());
                                rejected.setError(condition);
                                delivery.disposition(rejected);
                            } else {
                                if (receiver.getCredit() <= (producerCredit * .2)) {
                                    LOG.trace("Sending more credit ({}) to producer: {}", producerCredit - receiver.getCredit(), producerId);
                                    receiver.flow(producerCredit - receiver.getCredit());
                                }

                                if (remoteState != null && remoteState instanceof TransactionalState) {
                                    TransactionalState txAccepted = new TransactionalState();
                                    txAccepted.setOutcome(Accepted.getInstance());
                                    txAccepted.setTxnId(((TransactionalState) remoteState).getTxnId());

                                    delivery.disposition(txAccepted);
                                } else {
                                    delivery.disposition(Accepted.getInstance());
                                }

                                delivery.settle();
                            }

                            pumpProtonToSocket();
                        }
                    });
                } else {
                    if (receiver.getCredit() <= (producerCredit * .2)) {
                        LOG.trace("Sending more credit ({}) to producer: {}", producerCredit - receiver.getCredit(), producerId);
                        receiver.flow(producerCredit - receiver.getCredit());
                        pumpProtonToSocket();
                    }
                    sendToActiveMQ(message, null);
                }
            }
        }

        @Override
        public void onClose() throws Exception {
            if (!closed) {
                sendToActiveMQ(new RemoveInfo(producerId), null);
            }

            super.onClose();
        }

        public void close() {
            closed = true;
        }

        public boolean isAnonymous() {
            return anonymous;
        }
    }

    private final AtomicLong nextTransactionId = new AtomicLong();

    AmqpDeliveryListener coordinatorContext = new BaseProducerContext() {

        @Override
        protected void onMessage(Receiver receiver, final Delivery delivery, Buffer buffer) throws Exception {

            Message msg = Proton.message();
            int offset = buffer.offset;
            int len = buffer.length;
            while (len > 0) {
                final int decoded = msg.decode(buffer.data, offset, len);
                assert decoded > 0 : "Make progress decoding the message";
                offset += decoded;
                len -= decoded;
            }

            final Object action = ((AmqpValue) msg.getBody()).getValue();
            LOG.debug("COORDINATOR received: {}, [{}]", action, buffer);
            if (action instanceof Declare) {
                Declare declare = (Declare) action;
                if (declare.getGlobalId() != null) {
                    throw new Exception("don't know how to handle a declare /w a set GlobalId");
                }

                long txid = nextTransactionId.incrementAndGet();
                TransactionInfo txinfo = new TransactionInfo(connectionId, new LocalTransactionId(connectionId, txid), TransactionInfo.BEGIN);
                sendToActiveMQ(txinfo, null);
                LOG.trace("started transaction {}", txid);

                Declared declared = new Declared();
                declared.setTxnId(new Binary(toBytes(txid)));
                delivery.disposition(declared);
                delivery.settle();
            } else if (action instanceof Discharge) {
                Discharge discharge = (Discharge) action;
                long txid = toLong(discharge.getTxnId());

                final byte operation;
                if (discharge.getFail()) {
                    LOG.trace("rollback transaction {}", txid);
                    operation = TransactionInfo.ROLLBACK;
                } else {
                    LOG.trace("commit transaction {}", txid);
                    operation = TransactionInfo.COMMIT_ONE_PHASE;
                }

                AmqpSessionContext context = (AmqpSessionContext) receiver.getSession().getContext();
                for (ConsumerContext consumer : context.consumers.values()) {
                    if (operation == TransactionInfo.ROLLBACK) {
                        consumer.doRollback();
                    } else {
                        consumer.doCommit();
                    }
                }

                TransactionInfo txinfo = new TransactionInfo(connectionId, new LocalTransactionId(connectionId, txid), operation);
                sendToActiveMQ(txinfo, new ResponseHandler() {
                    @Override
                    public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            ExceptionResponse er = (ExceptionResponse) response;
                            Rejected rejected = new Rejected();
                            rejected.setError(new ErrorCondition(Symbol.valueOf("failed"), er.getException().getMessage()));
                            delivery.disposition(rejected);
                        } else {
                            delivery.disposition(Accepted.getInstance());
                        }
                        LOG.debug("TX: {} settling {}", operation, action);
                        delivery.settle();
                        pumpProtonToSocket();
                    }
                });

                for (ConsumerContext consumer : context.consumers.values()) {
                    if (operation == TransactionInfo.ROLLBACK) {
                        consumer.pumpOutbound();
                    }
                }

            } else {
                throw new Exception("Expected coordinator message type: " + action.getClass());
            }
        }
    };

    void onReceiverOpen(final Receiver receiver, AmqpSessionContext sessionContext) {
        // Client is producing to this receiver object
        org.apache.qpid.proton.amqp.transport.Target remoteTarget = receiver.getRemoteTarget();
        int flow = producerCredit;

        try {
            if (remoteTarget instanceof Coordinator) {
                pumpProtonToSocket();
                receiver.setContext(coordinatorContext);
                receiver.flow(flow);
                receiver.open();
                pumpProtonToSocket();
            } else {
                Target target = (Target) remoteTarget;
                ProducerId producerId = new ProducerId(sessionContext.sessionId, sessionContext.nextProducerId++);
                final ProducerContext producerContext = new ProducerContext(producerId);
                ActiveMQDestination destination = null;
                String targetNodeName = target.getAddress();

                if ((targetNodeName == null || targetNodeName.length() == 0) && !target.getDynamic()) {
                    producerContext.anonymous = true;
                } else if (target.getDynamic()) {
                    destination = createTemporaryDestination(receiver, target.getCapabilities());
                    Target actualTarget = new Target();
                    actualTarget.setAddress(destination.getQualifiedName());
                    actualTarget.setDynamic(true);
                    receiver.setTarget(actualTarget);
                    producerContext.addCloseAction(new Runnable() {

                        @Override
                        public void run() {
                            deleteTemporaryDestination((ActiveMQTempDestination) producerContext.getDestination());
                        }
                    });
                } else {
                    destination = createDestination(remoteTarget);
                }

                receiver.setContext(producerContext);
                receiver.flow(flow);

                ProducerInfo producerInfo = new ProducerInfo(producerId);
                producerInfo.setDestination(destination);
                producerContext.setDestination(destination);
                sendToActiveMQ(producerInfo, new ResponseHandler() {
                    @Override
                    public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            receiver.setTarget(null);
                            Throwable exception = ((ExceptionResponse) response).getException();
                            if (exception instanceof SecurityException) {
                                receiver.setCondition(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, exception.getMessage()));
                            } else {
                                receiver.setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, exception.getMessage()));
                            }
                            producerContext.closed = true;
                            receiver.close();
                            receiver.free();
                        } else {
                            receiver.open();
                        }
                        pumpProtonToSocket();
                    }
                });
            }
        } catch (AmqpProtocolException exception) {
            receiver.setTarget(null);
            receiver.setCondition(new ErrorCondition(Symbol.getSymbol(exception.getSymbolicName()), exception.getMessage()));
            receiver.close();
        }
    }

    private ActiveMQDestination createDestination(Object endpoint) throws AmqpProtocolException {
        if (endpoint == null) {
            return null;
        } else if (endpoint instanceof Coordinator) {
            return null;
        } else if (endpoint instanceof org.apache.qpid.proton.amqp.messaging.Terminus) {
            org.apache.qpid.proton.amqp.messaging.Terminus terminus = (org.apache.qpid.proton.amqp.messaging.Terminus) endpoint;
            if (terminus.getAddress() == null || terminus.getAddress().length() == 0) {
                if (terminus instanceof org.apache.qpid.proton.amqp.messaging.Source) {
                    throw new AmqpProtocolException("amqp:invalid-field", "source address not set");
                } else {
                    throw new AmqpProtocolException("amqp:invalid-field", "target address not set");
                }
            }

            return ActiveMQDestination.createDestination(terminus.getAddress(), ActiveMQDestination.QUEUE_TYPE);
        } else {
            throw new RuntimeException("Unexpected terminus type: " + endpoint);
        }
    }

    OutboundTransformer outboundTransformer = new AutoOutboundTransformer(ActiveMQJMSVendor.INSTANCE);

    class ConsumerContext extends AmqpDeliveryListener {
        private final ConsumerId consumerId;
        private final Sender sender;
        private final boolean presettle;
        private boolean closed;
        public ConsumerInfo info;
        private boolean endOfBrowse = false;
        public int credit;
        private long lastDeliveredSequenceId;

        protected LinkedList<MessageDispatch> dispatchedInTx = new LinkedList<MessageDispatch>();

        public ConsumerContext(ConsumerId consumerId, Sender sender) {
            this.consumerId = consumerId;
            this.sender = sender;
            this.presettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;
        }

        long nextTagId = 0;
        HashSet<byte[]> tagCache = new HashSet<byte[]>();

        byte[] nextTag() {
            byte[] rc;
            if (tagCache != null && !tagCache.isEmpty()) {
                final Iterator<byte[]> iterator = tagCache.iterator();
                rc = iterator.next();
                iterator.remove();
            } else {
                try {
                    rc = Long.toHexString(nextTagId++).getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
            return rc;
        }

        void checkinTag(byte[] data) {
            if (tagCache.size() < 1024) {
                tagCache.add(data);
            }
        }

        @Override
        public String toString() {
            return "ConsumerContext { " + info + " }";
        }

        @Override
        public void onDetach() throws Exception {
            if (!closed) {
                closed = true;
                sender.setContext(null);
                subscriptionsByConsumerId.remove(consumerId);

                AmqpSessionContext session = (AmqpSessionContext) sender.getSession().getContext();
                if (session != null) {
                    session.consumers.remove(info.getConsumerId());
                }

                RemoveInfo removeCommand = new RemoveInfo(consumerId);
                removeCommand.setLastDeliveredSequenceId(lastDeliveredSequenceId);
                sendToActiveMQ(removeCommand, null);
            }
        }

        @Override
        public void onClose() throws Exception {
            try {
                if (!closed) {
                    closed = true;
                    sender.setContext(null);
                    subscriptionsByConsumerId.remove(consumerId);

                    AmqpSessionContext session = (AmqpSessionContext) sender.getSession().getContext();
                    if (session != null) {
                        session.consumers.remove(info.getConsumerId());
                    }

                    RemoveInfo removeCommand = new RemoveInfo(consumerId);
                    removeCommand.setLastDeliveredSequenceId(lastDeliveredSequenceId);
                    sendToActiveMQ(removeCommand, null);

                    if (info.isDurable()) {
                        RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
                        rsi.setConnectionId(connectionId);
                        rsi.setSubscriptionName(sender.getName());
                        rsi.setClientId(connectionInfo.getClientId());

                        sendToActiveMQ(rsi, null);
                    }
                }
            } finally {
                super.onClose();
            }
        }

        LinkedList<MessageDispatch> outbound = new LinkedList<MessageDispatch>();

        // called when the connection receives a JMS message from ActiveMQ
        public void onMessageDispatch(MessageDispatch md) throws Exception {
            if (!closed) {
                // Lock to prevent stepping on TX redelivery
                synchronized (outbound) {
                    outbound.addLast(md);
                }
                pumpOutbound();
                pumpProtonToSocket();
            }
        }

        Buffer currentBuffer;
        Delivery currentDelivery;
        final String MESSAGE_FORMAT_KEY = outboundTransformer.getPrefixVendor() + "MESSAGE_FORMAT";

        public void pumpOutbound() throws Exception {
            while (!closed) {
                while (currentBuffer != null) {
                    int sent = sender.send(currentBuffer.data, currentBuffer.offset, currentBuffer.length);
                    if (sent > 0) {
                        currentBuffer.moveHead(sent);
                        if (currentBuffer.length == 0) {
                            if (presettle) {
                                settle(currentDelivery, MessageAck.INDIVIDUAL_ACK_TYPE);
                            } else {
                                sender.advance();
                            }
                            currentBuffer = null;
                            currentDelivery = null;
                        }
                    } else {
                        return;
                    }
                }

                if (outbound.isEmpty()) {
                    return;
                }

                final MessageDispatch md = outbound.removeFirst();
                try {

                    ActiveMQMessage temp = null;
                    if (md.getMessage() != null) {

                        // Topics can dispatch the same Message to more than one consumer
                        // so we must copy to prevent concurrent read / write to the same
                        // message object.
                        if (md.getDestination().isTopic()) {
                            synchronized (md.getMessage()) {
                                temp = (ActiveMQMessage) md.getMessage().copy();
                            }
                        } else {
                            temp = (ActiveMQMessage) md.getMessage();
                        }

                        if (!temp.getProperties().containsKey(MESSAGE_FORMAT_KEY)) {
                            temp.setProperty(MESSAGE_FORMAT_KEY, 0);
                        }
                    }

                    final ActiveMQMessage jms = temp;
                    if (jms == null) {
                        // It's the end of browse signal.
                        endOfBrowse = true;
                        drainCheck();
                    } else {
                        jms.setRedeliveryCounter(md.getRedeliveryCounter());
                        jms.setReadOnlyBody(true);
                        final EncodedMessage amqp = outboundTransformer.transform(jms);
                        if (amqp != null && amqp.getLength() > 0) {
                            currentBuffer = new Buffer(amqp.getArray(), amqp.getArrayOffset(), amqp.getLength());
                            if (presettle) {
                                currentDelivery = sender.delivery(EMPTY_BYTE_ARRAY, 0, 0);
                            } else {
                                final byte[] tag = nextTag();
                                currentDelivery = sender.delivery(tag, 0, tag.length);
                            }
                            currentDelivery.setContext(md);
                        } else {
                            // TODO: message could not be generated what now?
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void settle(final Delivery delivery, final int ackType) throws Exception {
            byte[] tag = delivery.getTag();
            if (tag != null && tag.length > 0 && delivery.remotelySettled()) {
                checkinTag(tag);
            }

            if (ackType == -1) {
                // we are going to settle, but redeliver.. we we won't yet ack
                // to ActiveMQ
                delivery.settle();
                onMessageDispatch((MessageDispatch) delivery.getContext());
            } else {
                MessageDispatch md = (MessageDispatch) delivery.getContext();
                lastDeliveredSequenceId = md.getMessage().getMessageId().getBrokerSequenceId();
                MessageAck ack = new MessageAck();
                ack.setConsumerId(consumerId);
                ack.setFirstMessageId(md.getMessage().getMessageId());
                ack.setLastMessageId(md.getMessage().getMessageId());
                ack.setMessageCount(1);
                ack.setAckType((byte) ackType);
                ack.setDestination(md.getDestination());

                DeliveryState remoteState = delivery.getRemoteState();
                if (remoteState != null && remoteState instanceof TransactionalState) {
                    TransactionalState s = (TransactionalState) remoteState;
                    long txid = toLong(s.getTxnId());
                    LocalTransactionId localTxId = new LocalTransactionId(connectionId, txid);
                    ack.setTransactionId(localTxId);

                    // Store the message sent in this TX we might need to
                    // re-send on rollback
                    md.getMessage().setTransactionId(localTxId);
                    dispatchedInTx.addFirst(md);
                }


                LOG.trace("Sending Ack to ActiveMQ: {}", ack);

                sendToActiveMQ(ack, new ResponseHandler() {
                    @Override
                    public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            if (response.isException()) {
                                Throwable exception = ((ExceptionResponse) response).getException();
                                exception.printStackTrace();
                                sender.close();
                            }
                        } else {
                            delivery.settle();
                        }
                        pumpProtonToSocket();
                    }
                });
            }
        }

        @Override
        public void drainCheck() {
            // If we are a browser.. lets not say we are drained until
            // we hit the end of browse message.
            if (info.isBrowser() && !endOfBrowse)
                return;

            if (outbound.isEmpty()) {
                sender.drained();
            }
        }

        @Override
        public void onDelivery(Delivery delivery) throws Exception {
            MessageDispatch md = (MessageDispatch) delivery.getContext();
            DeliveryState state = delivery.getRemoteState();

            if (state instanceof TransactionalState) {
                TransactionalState txState = (TransactionalState) state;
                LOG.trace("onDelivery: TX delivery state = {}", state);
                if (txState.getOutcome() != null) {
                    Outcome outcome = txState.getOutcome();
                    if (outcome instanceof Accepted) {
                        if (!delivery.remotelySettled()) {
                            TransactionalState txAccepted = new TransactionalState();
                            txAccepted.setOutcome(Accepted.getInstance());
                            txAccepted.setTxnId(((TransactionalState) state).getTxnId());

                            delivery.disposition(txAccepted);
                        }
                        settle(delivery, MessageAck.DELIVERED_ACK_TYPE);
                    }
                }
            } else {
                if (state instanceof Accepted) {
                    LOG.trace("onDelivery: accepted state = {}", state);
                    if (!delivery.remotelySettled()) {
                        delivery.disposition(new Accepted());
                    }
                    settle(delivery, MessageAck.INDIVIDUAL_ACK_TYPE);
                } else if (state instanceof Rejected) {
                    // re-deliver /w incremented delivery counter.
                    md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                    LOG.trace("onDelivery: Rejected state = {}, delivery count now {}", state, md.getRedeliveryCounter());
                    settle(delivery, -1);
                } else if (state instanceof Released) {
                    LOG.trace("onDelivery: Released state = {}", state);
                    // re-deliver && don't increment the counter.
                    settle(delivery, -1);
                } else if (state instanceof Modified) {
                    Modified modified = (Modified) state;
                    if (modified.getDeliveryFailed()) {
                        // increment delivery counter..
                        md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                    }
                    LOG.trace("onDelivery: Modified state = {}, delivery count now {}", state, md.getRedeliveryCounter());
                    byte ackType = -1;
                    Boolean undeliverableHere = modified.getUndeliverableHere();
                    if (undeliverableHere != null && undeliverableHere) {
                        // receiver does not want the message..
                        // perhaps we should DLQ it?
                        ackType = MessageAck.POSION_ACK_TYPE;
                    }
                    settle(delivery, ackType);
                }
            }
            pumpOutbound();
        }

        @Override
        void doCommit() throws Exception {
            if (!dispatchedInTx.isEmpty()) {
                for (MessageDispatch md : dispatchedInTx) {
                    MessageAck pendingTxAck = new MessageAck(md, MessageAck.INDIVIDUAL_ACK_TYPE, 1);
                    pendingTxAck.setFirstMessageId(md.getMessage().getMessageId());
                    pendingTxAck.setTransactionId(md.getMessage().getTransactionId());

                    LOG.trace("Sending commit Ack to ActiveMQ: {}", pendingTxAck);

                    sendToActiveMQ(pendingTxAck, new ResponseHandler() {
                        @Override
                        public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                            if (response.isException()) {
                                if (response.isException()) {
                                    Throwable exception = ((ExceptionResponse) response).getException();
                                    exception.printStackTrace();
                                    sender.close();
                                }
                            }
                            pumpProtonToSocket();
                        }
                    });
                }

                dispatchedInTx.clear();
            }
        }

        @Override
        void doRollback() throws Exception {
            synchronized (outbound) {

                LOG.trace("Rolling back {} messages for redelivery. ", dispatchedInTx.size());

                for (MessageDispatch md : dispatchedInTx) {
                    md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                    md.getMessage().setTransactionId(null);
                    outbound.addFirst(md);
                }

                dispatchedInTx.clear();
            }
        }
    }

    private final ConcurrentHashMap<ConsumerId, ConsumerContext> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, ConsumerContext>();

    @SuppressWarnings("unchecked")
    void onSenderOpen(final Sender sender, final AmqpSessionContext sessionContext) {
        org.apache.qpid.proton.amqp.messaging.Source source = (org.apache.qpid.proton.amqp.messaging.Source) sender.getRemoteSource();

        try {
            final Map<Symbol, Object> supportedFilters = new HashMap<Symbol, Object>();
            final ConsumerId id = new ConsumerId(sessionContext.sessionId, sessionContext.nextConsumerId++);
            final ConsumerContext consumerContext = new ConsumerContext(id, sender);
            sender.setContext(consumerContext);

            boolean noLocal = false;
            String selector = null;

            if (source != null) {
                Map.Entry<Symbol, DescribedType> filter = findFilter(source.getFilter(), JMS_SELECTOR_FILTER_IDS);
                if (filter != null) {
                    selector = filter.getValue().getDescribed().toString();
                    // Validate the Selector.
                    try {
                        SelectorParser.parse(selector);
                    } catch (InvalidSelectorException e) {
                        sender.setSource(null);
                        sender.setCondition(new ErrorCondition(AmqpError.INVALID_FIELD, e.getMessage()));
                        sender.close();
                        consumerContext.closed = true;
                        return;
                    }

                    supportedFilters.put(filter.getKey(), filter.getValue());
                }

                filter = findFilter(source.getFilter(), NO_LOCAL_FILTER_IDS);
                if (filter != null) {
                    noLocal = true;
                    supportedFilters.put(filter.getKey(), filter.getValue());
                }
            }

            ActiveMQDestination destination;
            if (source == null) {
                // Attempt to recover previous subscription
                destination = lookupSubscription(sender.getName());

                if (destination != null) {
                    source = new org.apache.qpid.proton.amqp.messaging.Source();
                    source.setAddress(destination.getQualifiedName());
                    source.setDurable(TerminusDurability.UNSETTLED_STATE);
                    source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
                    source.setDistributionMode(COPY);
                } else {
                    consumerContext.closed = true;
                    sender.setSource(null);
                    sender.setCondition(new ErrorCondition(AmqpError.NOT_FOUND, "Unknown subscription link: " + sender.getName()));
                    sender.close();
                    sender.free();
                    pumpProtonToSocket();
                    return;
                }
            } else if (source.getDynamic()) {
                // lets create a temp dest.
                destination = createTemporaryDestination(sender, source.getCapabilities());
                source = new org.apache.qpid.proton.amqp.messaging.Source();
                source.setAddress(destination.getQualifiedName());
                source.setDynamic(true);
                consumerContext.addCloseAction(new Runnable() {

                    @Override
                    public void run() {
                        deleteTemporaryDestination((ActiveMQTempDestination) consumerContext.getDestination());
                    }
                });
            } else {
                destination = createDestination(source);
            }

            source.setFilter(supportedFilters.isEmpty() ? null : supportedFilters);
            sender.setSource(source);

            int senderCredit = sender.getRemoteCredit();

            subscriptionsByConsumerId.put(id, consumerContext);
            ConsumerInfo consumerInfo = new ConsumerInfo(id);
            consumerInfo.setSelector(selector);
            consumerInfo.setNoRangeAcks(true);
            consumerInfo.setDestination(destination);
            consumerInfo.setPrefetchSize(senderCredit >= 0 ? senderCredit : 0);
            consumerInfo.setDispatchAsync(true);
            consumerInfo.setNoLocal(noLocal);

            if (source.getDistributionMode() == COPY && destination.isQueue()) {
                consumerInfo.setBrowser(true);
            }
            if ((TerminusDurability.UNSETTLED_STATE.equals(source.getDurable()) ||
                 TerminusDurability.CONFIGURATION.equals(source.getDurable())) && destination.isTopic()) {
                consumerInfo.setSubscriptionName(sender.getName());
            }

            consumerContext.info = consumerInfo;
            consumerContext.setDestination(destination);
            consumerContext.credit = senderCredit;

            sendToActiveMQ(consumerInfo, new ResponseHandler() {
                @Override
                public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                    if (response.isException()) {
                        sender.setSource(null);
                        Throwable exception = ((ExceptionResponse) response).getException();
                        if (exception instanceof SecurityException) {
                            sender.setCondition(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, exception.getMessage()));
                        } else if (exception instanceof InvalidSelectorException) {
                            sender.setCondition(new ErrorCondition(AmqpError.INVALID_FIELD, exception.getMessage()));
                        } else {
                            sender.setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, exception.getMessage()));
                        }
                        subscriptionsByConsumerId.remove(id);
                        consumerContext.closed = true;
                        sender.close();
                        sender.free();
                    } else {
                        sessionContext.consumers.put(id, consumerContext);
                        sender.open();
                    }
                    pumpProtonToSocket();
                }
            });
        } catch (AmqpProtocolException e) {
            sender.setSource(null);
            sender.setCondition(new ErrorCondition(Symbol.getSymbol(e.getSymbolicName()), e.getMessage()));
            sender.close();
        }
    }

    private ActiveMQDestination lookupSubscription(String subscriptionName) throws AmqpProtocolException {
        ActiveMQDestination result = null;
        RegionBroker regionBroker;

        try {
            regionBroker = (RegionBroker) brokerService.getBroker().getAdaptor(RegionBroker.class);
        } catch (Exception e) {
            throw new AmqpProtocolException("Error finding subscription: " + subscriptionName + ": " + e.getMessage(), false, e);
        }

        final TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();
        DurableTopicSubscription subscription = topicRegion.lookupSubscription(subscriptionName, connectionInfo.getClientId());
        if (subscription != null) {
            result = subscription.getActiveMQDestination();
        }

        return result;
    }

    private ActiveMQDestination createTemporaryDestination(final Link link, Symbol[] capabilities) {
        ActiveMQDestination rc = null;
        if (contains(capabilities, TEMP_TOPIC_CAPABILITY)) {
            rc = new ActiveMQTempTopic(connectionId, nextTempDestinationId++);
        } else if (contains(capabilities, TEMP_QUEUE_CAPABILITY)) {
            rc = new ActiveMQTempQueue(connectionId, nextTempDestinationId++);
        } else {
            LOG.debug("Dynamic link request with no type capability, defaults to Temporary Queue");
            rc = new ActiveMQTempQueue(connectionId, nextTempDestinationId++);
        }

        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(connectionId);
        info.setOperationType(DestinationInfo.ADD_OPERATION_TYPE);
        info.setDestination(rc);

        sendToActiveMQ(info, new ResponseHandler() {

            @Override
            public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                if (response.isException()) {
                    link.setSource(null);

                    Throwable exception = ((ExceptionResponse) response).getException();
                    if (exception instanceof SecurityException) {
                        link.setCondition(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, exception.getMessage()));
                    } else {
                        link.setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, exception.getMessage()));
                    }

                    link.close();
                    link.free();
                }
            }
        });

        return rc;
    }

    private void deleteTemporaryDestination(ActiveMQTempDestination destination) {
        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(connectionId);
        info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
        info.setDestination(destination);

        sendToActiveMQ(info, new ResponseHandler() {

            @Override
            public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                if (response.isException()) {
                    Throwable exception = ((ExceptionResponse) response).getException();
                    LOG.debug("Error during temp destination removeal: {}", exception.getMessage());
                }
            }
        });
    }

    // //////////////////////////////////////////////////////////////////////////
    //
    // Implementation methods
    //
    // //////////////////////////////////////////////////////////////////////////

    private final AtomicInteger lastCommandId = new AtomicInteger();
    private final ConcurrentMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<Integer, ResponseHandler>();

    void sendToActiveMQ(Command command, ResponseHandler handler) {
        command.setCommandId(lastCommandId.incrementAndGet());
        if (handler != null) {
            command.setResponseRequired(true);
            resposeHandlers.put(Integer.valueOf(command.getCommandId()), handler);
        }
        amqpTransport.sendToActiveMQ(command);
    }

    void handleException(Throwable exception) {
        exception.printStackTrace();
        LOG.debug("Exception detail", exception);
        try {
            amqpTransport.stop();
        } catch (Throwable e) {
            LOG.error("Failed to stop AMQP Transport ", e);
        }
    }

    @Override
    public void setProducerCredit(int producerCredit) {
        this.producerCredit = producerCredit;
    }

    @SuppressWarnings("unused")
    private List<SubscriptionInfo> lookupSubscriptions() throws AmqpProtocolException {
        List<SubscriptionInfo> subscriptions = Collections.emptyList();
        try {
            subscriptions = PersistenceAdapterSupport.listSubscriptions(brokerService.getPersistenceAdapter(), connectionInfo.getClientId());
        } catch (IOException e) {
            throw new AmqpProtocolException("Error loading store subscriptions", true, e);
        }

        return subscriptions;
    }

    public boolean tryAuthenticate(ConnectionInfo info, X509Certificate[] peerCertificates) {
        try {
            if (getAuthenticator().authenticate(info.getUserName(), info.getPassword(), peerCertificates) != null) {
                return true;
            }

            return false;
        } catch (Throwable error) {
            return false;
        }
    }

    private AuthenticationBroker getAuthenticator() {
        if (authenticator == null) {
            try {
                authenticator = (AuthenticationBroker) brokerService.getBroker().getAdaptor(AuthenticationBroker.class);
            } catch (Exception e) {
                LOG.debug("Failed to lookup AuthenticationBroker from Broker, will use a default Noop version.");
            }

            if (authenticator == null) {
                authenticator = new DefaultAuthenticationBroker();
            }
        }

        return authenticator;
    }

    private class DefaultAuthenticationBroker implements AuthenticationBroker {

        @Override
        public SecurityContext authenticate(String username, String password, X509Certificate[] peerCertificates) throws SecurityException {
            return new SecurityContext(username) {

                @Override
                public Set<Principal> getPrincipals() {
                    return null;
                }
            };
        }
    }
}
