package protocols.publishsubscribe;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolNotificationHandler;
import babel.handlers.ProtocolRequestHandler;
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
import protocols.multipaxos.messages.RequestForOrderMessage;
import protocols.multipaxos.notifications.DecideNotification;
import protocols.multipaxos.notifications.LeaderNotification;
import protocols.multipaxos.requests.ProposeRequest;
import protocols.publishsubscribe.messages.GiveMeYourReplicasMessage;
import protocols.publishsubscribe.messages.StateTransferRequestMessage;
import protocols.publishsubscribe.messages.StateTransferResponseMessage;
import protocols.publishsubscribe.messages.TakeMyReplicasMessage;
import protocols.publishsubscribe.notifications.OwnerNotification;
import protocols.publishsubscribe.notifications.PBDeliver;
import protocols.publishsubscribe.requests.*;
import utils.PropertiesUtils;
import utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;


public class PublishSubscribe extends GenericProtocol implements INotificationConsumer {
    final static Logger logger = LogManager.getLogger(PublishSubscribe.class.getName());

    public static final short PROTOCOL_ID = 1000;
    private static final int INITIAL_CAPACITY = 100;
    private static final String PROTOCOL_NAME = "Publish/Subscriber";
    private static final String MULTIPAXOS_CONTACT = "multipaxos_contact";
    private static final String REPLICA = "replica";

    private Map<String, Boolean> topics;
    private Map<String, List<String>> waiting;
    private boolean isReplica;
    private Host leader;
    private List<Host> membership;
    private Map<String, List<String>> unordered;

    private final ProtocolMessageHandler uponTakeMyReplicasMessage = protocolMessage -> {
        logger.info(myself + "upon take my replicas");
        TakeMyReplicasMessage m = (TakeMyReplicasMessage) protocolMessage;
        String topic = m.getTopic();
        Host replica = pickRandomFromMembership(m.getReplicas());
        List<String> toOrder = this.waiting.remove(topic);

        if (toOrder != null) {
            logger.info(myself + " Sending request for ordering to" + replica);
            sendMessageSideChannel(new RequestForOrderMessage(topic, toOrder), replica);
        }

    };

    private final ProtocolNotificationHandler uponLeaderNotification = (protocolNotification) -> {
        LeaderNotification notification = (LeaderNotification) protocolNotification;
        this.leader = notification.getLeader();
    };

    private final ProtocolMessageHandler uponGiveMeYourReplicasMessage = protocolMessage -> {
        GiveMeYourReplicasMessage m = (GiveMeYourReplicasMessage) protocolMessage;
        logger.info(myself + " Replicas requst from " + m.getFrom() + "to topic :" + m.getTopic());
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

    private TreeSet<Operation> message = new TreeSet<>();
    private int paxosInstaces = 0;

    private final ProtocolNotificationHandler uponOrderDecideNotification = (protocolNotification) -> {
        logger.info("Decide Notification");
        DecideNotification notification = (DecideNotification) protocolNotification;
        Operation operation = notification.getOperation();

        message.add(operation);
        // ver que tipo de operacao e
        executeOperations();
    };

    PersistentMap<String> messages = new PersistentMap<>(myself.toString());
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


    private final ProtocolMessageHandler uponRequestForOrderMessage = (protocolMessage) -> {
        RequestForOrderMessage message = (RequestForOrderMessage) protocolMessage;
        String topic = message.getTopic();
        List<String> messages = message.getMessages();
        List<String> unorderedList = this.unordered.getOrDefault(topic, new LinkedList<>());
        unorderedList.addAll(messages);
        requestOrdering(topic, messages);
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

        int currentSeq = lastMessagesDelivered.getOrDefault(topic, new Integer(0));
        currentSeq++;
        if (currentSeq == seq) {
            if (this.topics.containsKey(topic)) {
                triggerNotification(new PBDeliver(deliver.getMessage(), topic));
            }
            lastMessagesDelivered.put(topic, seq);

        }

    };

    public PublishSubscribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        // Notifications produced
        registerNotification(PBDeliver.NOTIFICATION_ID, PBDeliver.NOTIFICATION_NAME);

        registerNotificationHandler(MultiPaxos.PROTOCOL_ID, StartRequestNotification.NOTIFICATION_ID, uponStartRequestNotification);
        // Requests
        registerRequestHandler(PublishRequest.REQUEST_ID, uponPublishRequest);
        registerRequestHandler(SubscribeRequest.REQUEST_ID, uponSubscribeRequest);

        registerNotificationHandler(MultiPaxos.PROTOCOL_ID, DecideNotification.NOTIFICATION_ID, uponOrderDecideNotification);
        registerNotificationHandler(MultiPaxos.PROTOCOL_ID, LeaderNotification.NOTIFICATION_ID, uponLeaderNotification);
        registerMessageHandler(GiveMeYourReplicasMessage.MSG_CODE, uponGiveMeYourReplicasMessage, GiveMeYourReplicasMessage.serializer);
        registerMessageHandler(TakeMyReplicasMessage.MSG_CODE, uponTakeMyReplicasMessage, TakeMyReplicasMessage.serializer);
        registerMessageHandler(RequestForOrderMessage.MSG_CODE, uponRequestForOrderMessage, RequestForOrderMessage.serializer);
        registerMessageHandler(StateTransferRequestMessage.MSG_CODE, uponStateTransferRequestMessage, StateTransferRequestMessage.serializer);
        registerMessageHandler(StateTransferResponseMessage.MSG_CODE, uponStateTransferResponseMessage, StateTransferResponseMessage.serializer);

    }

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


    private void requestOrdering(String topic, List<String> messages) {
        logger.info("RequestOrdering " + messages + myself);
        WriteContent content = new WriteContent(topic, messages);
        Operation orderOp = new Operation(Utils.generateId(), Operation.Type.WRITE, content);
        ProposeRequest request = new ProposeRequest(orderOp);
        request.setDestination(MultiPaxos.PROTOCOL_ID);
        sendRequestToProtocol(request);
    }

    @Override
    public void init(Properties properties) {
        this.topics = new HashMap<>(INITIAL_CAPACITY);
        this.leader = null;
        this.waiting = new HashMap<>(64);
        this.unordered = new HashMap<>(64);
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
    }

    private void executeOperations() {
        Operation op = message.first();
        if (op.getInstance() == paxosInstaces + 1) {
            paxosInstaces++;
            MembershipUpdateContent muc;
            switch (op.getType()) {
                case ADD_REPLICA:
                    muc = (MembershipUpdateContent) (op.getContent());
                    this.membership.add(muc.getReplica());
                    break;
                case REMOVE_REPLICA:
                    muc = (MembershipUpdateContent) (op.getContent());
                    this.membership.remove(muc.getReplica());
                    break;
                case WRITE:
                    WriteContent wc = (WriteContent) op.getContent();
                    String topic = wc.getTopic();
                    for (String message : wc.getMessages()) {
                        logger.info("Scribbing " + message);
                        int seq = messages.put(topic, message);

                        if (!isReplica) {
                            DisseminatePubRequest disseminatePubRequest =
                                    new DisseminatePubRequest(topic, message, seq);

                            disseminatePubRequest.setDestination(Scribe.PROTOCOL_ID);
                            sendRequestToProtocol(disseminatePubRequest);
                        }
                    }
                    break;
            }
            message.remove(op);
        }
    }

    private Host pickRandomFromMembership(List<Host> membership) {
        int size = membership.size();
        int idx = Math.abs(r.nextInt() % size);

        return membership.get(idx);
    }

    private void sendRequestDecider(ProtocolRequest request, short PROTOCOL_ID) {
        logger.info(String.format("%s - Sending message", myself));
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
