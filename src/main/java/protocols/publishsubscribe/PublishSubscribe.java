package protocols.publishsubscribe;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolNotificationHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.notification.INotificationConsumer;
import babel.protocol.GenericProtocol;
import babel.requestreply.ProtocolRequest;
import network.Host;
import network.INetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import persistence.PersistentMap;
import protocols.dht.Chord;
import protocols.dht.notifications.MessageDeliver;
import protocols.dissemination.Scribe;
import protocols.dissemination.requests.DisseminatePubRequest;
import protocols.dissemination.requests.DisseminateSubRequest;
import protocols.multipaxos.MembershipUpdateContent;
import protocols.multipaxos.MultiPaxos;
import protocols.multipaxos.Operation;
import protocols.multipaxos.WriteContent;
import protocols.multipaxos.messages.RequestForOrderOperation;
import protocols.multipaxos.notifications.DecideNotification;
import protocols.multipaxos.notifications.LeaderNotification;
import protocols.multipaxos.requests.ProposeRequest;
import protocols.publishsubscribe.messages.*;
import protocols.publishsubscribe.notifications.OwnerNotification;
import protocols.publishsubscribe.notifications.PBDeliver;
import protocols.publishsubscribe.requests.*;
import protocols.publishsubscribe.timers.OrderMessageCheckerTimer;
import utils.PropertiesUtils;
import utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class PublishSubscribe extends GenericProtocol implements INotificationConsumer {
    final static Logger logger = LogManager.getLogger(PublishSubscribe.class.getName());

    public static final short PROTOCOL_ID = 1000;
    private static final String PROTOCOL_NAME = "Publish/Subscriber";

    private static final String MULTIPAXOS_CONTACT = "multipaxos_contact";
    private static final String TIMER_INIT = "check_order_timer_init";
    private static final String TIMER_PERIOD = "check_order_timer_period";
    private static final int INITIAL_CAPACITY = 100;

    private static final String REPLICA = "replica";
    public static final String OPERATIONS = "operations";

    private Map<String, Boolean> topics;
    private Map<String, List<String>> waiting;
    private boolean isReplica;
    private Host leader;
    private List<Host> membership;
    private Map<String, Integer> topicRelatedSeq;
    private Set<Operation> unordered;
    private TreeSet<Operation> operationsToBeExecuted;
    private int paxosInstaces;
    private PersistentMap<String, Operation> messages;

    public PublishSubscribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        // Notifications produced
        registerNotification(PBDeliver.NOTIFICATION_ID, PBDeliver.NOTIFICATION_NAME);
        registerNotificationHandler(MultiPaxos.PROTOCOL_ID, StartRequestNotification.NOTIFICATION_ID, uponStartRequestNotification);

        registerRequestHandler(PublishRequest.REQUEST_ID, uponPublishRequest);
        registerRequestHandler(SubscribeRequest.REQUEST_ID, uponSubscribeRequest);
        registerNotificationHandler(MultiPaxos.PROTOCOL_ID, DecideNotification.NOTIFICATION_ID, uponOrderDecideNotification);
        registerNotificationHandler(MultiPaxos.PROTOCOL_ID, LeaderNotification.NOTIFICATION_ID, uponLeaderNotification);
        registerMessageHandler(GiveMeYourReplicasMessage.MSG_CODE, uponGiveMeYourReplicasMessage, GiveMeYourReplicasMessage.serializer);
        registerMessageHandler(TakeMyReplicasMessage.MSG_CODE, uponTakeMyReplicasMessage, TakeMyReplicasMessage.serializer);
        registerMessageHandler(RequestForOrderOperation.MSG_CODE, uponRequestForOrderMessage, RequestForOrderOperation.serializer);
        registerMessageHandler(StateTransferRequestMessage.MSG_CODE, uponStateTransferRequestMessage, StateTransferRequestMessage.serializer);
        registerMessageHandler(StateTransferResponseMessage.MSG_CODE, uponStateTransferResponseMessage, StateTransferResponseMessage.serializer);

        registerMessageHandler(LatePaxosInstanceRequestMessage.MSG_CODE, uponLatePaxosInstanceMessage, LatePaxosInstanceRequestMessage.serializer);
        registerMessageHandler(LatePaxosInstanceReplyMessage.MSG_CODE, uponLatePaxosInstanceReplyMessage, LatePaxosInstanceReplyMessage.serializer);

        //Timers
        registerTimerHandler(OrderMessageCheckerTimer.TimerCode, uponOrderMessageCheckerTimer);
    }

    @Override
    public void init(Properties properties) {
        this.topics = new HashMap<>(INITIAL_CAPACITY);
        this.leader = null;
        this.waiting = new HashMap<>(64);
        this.unordered = new HashSet<>(64);
        this.topicRelatedSeq = new HashMap<>(64);
        operationsToBeExecuted = new TreeSet<>();
        this.messages = new PersistentMap<>(myself.toString());
        this.membership = new LinkedList<>();
        this.membership.add(myself);
        this.r = new Random();
        this.isReplica = PropertiesUtils.getPropertyAsBool(properties, REPLICA);

        if (!isReplica) {
            try {
                registerNotificationHandler(Scribe.PROTOCOL_ID, MessageDeliver.NOTIFICATION_ID, deliverNotification);
                registerNotificationHandler(Chord.PROTOCOL_ID, OwnerNotification.NOTIFICATION_ID, uponOwnerNotification);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        initMultiPaxos(properties);

        long timerInit = Long.parseLong(properties.getProperty(TIMER_INIT));
        long timerPeriod = Long.parseLong(properties.getProperty(TIMER_PERIOD));
        setupPeriodicTimer(new OrderMessageCheckerTimer(), timerInit, timerPeriod);
    }

    private final ProtocolMessageHandler uponTakeMyReplicasMessage = protocolMessage -> {
        logger.info(myself + "upon take my replicas");
        TakeMyReplicasMessage m = (TakeMyReplicasMessage) protocolMessage;
        String topic = m.getTopic();
        List<String> toOrder = this.waiting.remove(topic);

        if (toOrder != null) {
            Host replica = pickRandomFromMembership(m.getReplicas());
            WriteContent content = new WriteContent(topic, toOrder);
            Operation orderOp = new Operation(Utils.generateId(), Operation.Type.WRITE, content);
            logger.info(myself + " Sending request for ordering to" + replica);
            sendMessageSideChannel(new RequestForOrderOperation(orderOp), replica);
        }

    };

    private final ProtocolTimerHandler uponOrderMessageCheckerTimer = (protocolTimer) -> {
        List<Operation> currentOperationsToBeExecuted = new LinkedList<>(operationsToBeExecuted);

        if (currentOperationsToBeExecuted.isEmpty())
            return;

        Operation nextOperationWaiting = currentOperationsToBeExecuted.get(0);
        int nextPaxosInstance = this.paxosInstaces + 1;
        if (nextOperationWaiting.getInstance() != nextPaxosInstance) {
            logger.info(myself + "has late operations");
            Host replica = pickRandomFromMembership(membership);

            sendMessageSideChannel(new LatePaxosInstanceRequestMessage(nextPaxosInstance,
                    nextOperationWaiting.getInstance()), replica);
        }
    };

    private final ProtocolNotificationHandler uponLeaderNotification = (protocolNotification) -> {
        LeaderNotification notification = (LeaderNotification) protocolNotification;
        this.leader = notification.getLeader();

        if (notification.getPaxosInstance() > this.paxosInstaces) {
            this.paxosInstaces = notification.getPaxosInstance();
        }

        for (Operation op : this.unordered) {
            if (amILeader()) {
                ProposeRequest request = new ProposeRequest(op);
                request.setDestination(MultiPaxos.PROTOCOL_ID);
                sendRequestToProtocol(request);
            } else {
                sendMessage(new RequestForOrderOperation(op), this.leader);
            }

        }
    };

    private final ProtocolMessageHandler uponRequestForOrderMessage = (protocolMessage) -> {
        RequestForOrderOperation message = (RequestForOrderOperation) protocolMessage;
        Operation orderOp = message.getOperation();
        this.unordered.add(orderOp);
        if (amILeader()) {
            ProposeRequest request = new ProposeRequest(orderOp);
            request.setDestination(MultiPaxos.PROTOCOL_ID);
            sendRequestToProtocol(request);
        } else {
            sendMessage(message, this.leader);
        }
    };

    private final ProtocolMessageHandler uponGiveMeYourReplicasMessage = protocolMessage -> {
        GiveMeYourReplicasMessage m = (GiveMeYourReplicasMessage) protocolMessage;
        logger.info(myself + " Replicas request from " + m.getFrom() + "to topic :" + m.getTopic());
        sendMessageSideChannel(new TakeMyReplicasMessage(m.getTopic(), membership), m.getFrom());
    };

    private final ProtocolMessageHandler uponStateTransferRequestMessage = (protocolMessage) -> {
        try {
            this.messages.getState();

        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    private final ProtocolMessageHandler uponStateTransferResponseMessage = (protocolMessage) -> {
        StateTransferResponseMessage message = (StateTransferResponseMessage) protocolMessage;
        this.messages.setState(message.getState());
    };

    private final ProtocolNotificationHandler uponStartRequestNotification = (protocolNotifcation) -> {
        StartRequestNotification notification = (StartRequestNotification) protocolNotifcation;
        this.leader = notification.getLeader();
        this.membership = notification.getMembership();
        this.paxosInstaces = notification.getPaxosInstance();

        Host replicaToExecuteStateTransfer = pickRandomFromMembership(this.membership);
        sendMessageSideChannel(new StateTransferRequestMessage(), replicaToExecuteStateTransfer);
    };

    private final ProtocolNotificationHandler uponOrderDecideNotification = (protocolNotification) -> {
        logger.info("Decide Notification");
        DecideNotification notification = (DecideNotification) protocolNotification;
        Operation operation = notification.getOperation();

        this.unordered.remove(operation);

        operationsToBeExecuted.add(operation);
        executeOperations();
    };

    private final ProtocolMessageHandler uponLatePaxosInstanceMessage = (protocolMessage) -> {
        logger.info("Received a late request message from " + protocolMessage.getFrom());
        LatePaxosInstanceRequestMessage message = (LatePaxosInstanceRequestMessage) protocolMessage;
        try {
            sendMessageSideChannel(new LatePaxosInstanceReplyMessage(messages.get(OPERATIONS,
                    message.getStart(), message.getEnd())), protocolMessage.getFrom());
        } catch (Exception e) {
            logger.error(e);
            logger.error("Could not send late operations");
        }
    };

    private final ProtocolMessageHandler uponLatePaxosInstanceReplyMessage = (protocolMessage) -> {
        logger.info("Received a late request message from " + protocolMessage.getFrom());
        LatePaxosInstanceReplyMessage message = (LatePaxosInstanceReplyMessage) protocolMessage;

        for (Operation op : message.getOperations()) {
            messages.put(OPERATIONS, op);
        }

    };

    /**
     * Fill the map with the client's subscribed topics or remove them.
     */
    private ProtocolRequestHandler uponSubscribeRequest = (protocolRequest) -> {
        SubscribeRequest subscribeRequest = (SubscribeRequest) protocolRequest;
        String topic = subscribeRequest.getTopic();
        boolean isSubscribe = subscribeRequest.isSubscribe();

        if (this.topics.get(topic) == null) {
            if (isSubscribe) {
                this.topics.put(topic, true);
            }
        } else {
            if (!isSubscribe) {
                this.topics.remove(topic);
            }
        }

        DisseminateSubRequest disseminateSubRequest = new DisseminateSubRequest(topic, isSubscribe);
        sendRequestDecider(disseminateSubRequest, Scribe.PROTOCOL_ID);
    };

    private void initMultiPaxos(Properties properties) {
        StartRequest request = new StartRequest();
        request.setDestination(MultiPaxos.PROTOCOL_ID);
        String rawContacts = PropertiesUtils.getPropertyAsString(properties, MULTIPAXOS_CONTACT);

        if (rawContacts != null) {
            String[] multipaxosContact = rawContacts.split(":");
            request.setContact(getHost(multipaxosContact));
        }

        sendRequestToProtocol(request);
    }

    private Host getHost(String[] contact) {
        try {
            return new Host(InetAddress.getByName(contact[0]), Integer.parseInt(contact[1]));
        } catch (UnknownHostException e) {
            // Ignored
            e.printStackTrace();
        }
        return null;
    }

    private Random r;
    private Map<String, Integer> lastMessagesDelivered = new HashMap<>();

    /**
     * Triggers a notification to the client.
     *
     * @param pNotification to be delivered.
     */
    public ProtocolNotificationHandler deliverNotification = (pNotification) -> {
        logger.info("Delivering notification");
        MessageDeliver deliver = (MessageDeliver) pNotification;
        String topic = deliver.getTopic();

        int seq = deliver.getSeq();

        //se receber por ordem posso entregar logo caso contrario preciso de fazer getMessage e quando chegar as cenas que preenchem a gap dou deliver

        int currentSeq = lastMessagesDelivered.getOrDefault(topic, seq - 1);
        currentSeq++;
        if (currentSeq == seq) {
            logger.info("Executing notification");
            if (this.topics.containsKey(topic)) {
                triggerNotification(new PBDeliver(deliver.getMessage(), topic));
            }
            lastMessagesDelivered.put(topic, seq);
        }

    };

    /**
     * Sends a publish requests to the underlying protocol.
     */
    private ProtocolRequestHandler uponPublishRequest = (publishRequest) -> {
        PublishRequest pRequest = (PublishRequest) publishRequest;
        FindOwnerRequest request = new FindOwnerRequest(pRequest.getTopic());

        logger.info("Publishing request " + pRequest.getMessage());

        List<String> message = waiting.get(pRequest.getTopic());
        if (message == null) {
            sendRequestDecider(request, Chord.PROTOCOL_ID);
            message = new LinkedList<>();
            waiting.put(pRequest.getTopic(), message);
        }

        message.add(pRequest.getMessage());
    };
    private ProtocolNotificationHandler uponOwnerNotification = protocolNotification -> {
        OwnerNotification notification = (OwnerNotification) protocolNotification;
        logger.info("Owner Notification  Trigger for topic :" + notification.getTopic());
        sendMessageSideChannel(new GiveMeYourReplicasMessage(notification.getTopic()),
                notification.getOwner());
    };

    private boolean amILeader() {
        return myself.equals(this.leader);
    }

    private void executeOperations() {
        Operation op = operationsToBeExecuted.first();

        if (op.getInstance() == paxosInstaces + 1) {
            logger.info("Paxos instance matched");
            paxosInstaces++;
            MembershipUpdateContent muc;
            switch (op.getType()) {
                case ADD_REPLICA:
                    muc = (MembershipUpdateContent) (op.getContent());
                    if (!this.membership.contains(muc.getReplica())) {
                        this.membership.add(muc.getReplica());
                    }
                    break;
                case REMOVE_REPLICA:
                    muc = (MembershipUpdateContent) (op.getContent());
                    this.membership.remove(muc.getReplica());
                    break;
                case WRITE:
                    WriteContent wc = (WriteContent) op.getContent();
                    String topic = wc.getTopic();

                    messages.put(OPERATIONS, op);
                    for (String message : wc.getMessages()) {
                        logger.info("Scribbing " + message);

                        if (!isReplica) {
                            int seq = topicRelatedSeq.getOrDefault(topic, 0);
                            seq++;
                            topicRelatedSeq.put(topic, seq);
                            DisseminatePubRequest disseminatePubRequest =
                                    new DisseminatePubRequest(topic, message, seq);

                            disseminatePubRequest.setDestination(Scribe.PROTOCOL_ID);
                            sendRequestToProtocol(disseminatePubRequest);
                        }
                    }
                    break;
            }
            operationsToBeExecuted.remove(op);
        } else {
            logger.info("Paxos instance did not match");
        }
    }

    private Host pickRandomFromMembership(List<Host> membership) {
        int size = membership.size();
        int idx = Math.abs(r.nextInt() % size);

        return membership.get(idx);
    }

    private void sendRequestDecider(ProtocolRequest request, short PROTOCOL_ID) {
        logger.info(String.format("%s - Sending operationsToBeExecuted", myself));
        request.setDestination(PROTOCOL_ID);
        sendRequestToProtocol(request);
    }

    private void sendRequestToProtocol(ProtocolRequest request) {
        try {
            this.sendRequest(request);
        } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
            logger.error(destinationProtocolDoesNotExist);
        }
    }

}
