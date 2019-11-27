package protocols.publishsubscribe;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolNotificationHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
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
import protocols.multipaxos.MultiPaxos;
import protocols.multipaxos.OrderOperation;
import protocols.multipaxos.messages.OperationMessage;
import protocols.multipaxos.messages.RequestForOrderMessage;
import protocols.multipaxos.notifications.DecideNotification;
import protocols.multipaxos.requests.ProposeRequest;
import protocols.publishsubscribe.messages.GiveMeYourReplicasMessage;
import protocols.publishsubscribe.messages.TakeMyReplicasMessage;
import protocols.publishsubscribe.notifications.OwnerNotification;
import protocols.publishsubscribe.notifications.PBDeliver;
import protocols.publishsubscribe.requests.FindOwnerRequest;
import protocols.publishsubscribe.requests.PublishRequest;
import protocols.publishsubscribe.requests.StartRequest;
import protocols.publishsubscribe.requests.SubscribeRequest;
import utils.PropertiesUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static com.sun.tools.javadoc.Main.execute;

public class PublishSubscribe extends GenericProtocol implements INotificationConsumer {
    final static Logger logger = LogManager.getLogger(PublishSubscribe.class.getName());

    public static final short PROTOCOL_ID = 1000;
    private static final int INITIAL_CAPACITY = 100;
    private static final String PROTOCOL_NAME = "Publish/Subscriber";
    private static final String MULTIPAXOS_CONTACT = "MultipaxosContact";

    private Map<String, Boolean> topics;
    private Host multiPaxosLeader;
    private Map<String, List<String>> waiting;
    private boolean isReplica;
    private Host leader;
    private List<Host> membership;
    private Map<String, List<String>> unordered;

    public PublishSubscribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        // Notifications produced
        registerNotification(PBDeliver.NOTIFICATION_ID, PBDeliver.NOTIFICATION_NAME);

        // Requests
        registerRequestHandler(PublishRequest.REQUEST_ID, uponPublishRequest);
        registerRequestHandler(SubscribeRequest.REQUEST_ID, uponSubscribeRequest);
        registerNotificationHandler(Chord.PROTOCOL_ID, OwnerNotification.NOTIFICATION_ID, uponOwnerNotification);
        registerNotificationHandler(MultiPaxos.PROTOCOL_ID, DecideNotification.NOTIFICATION_ID, uponOrderDecideNotification);
        registerMessageHandler(GiveMeYourReplicasMessage.MSG_CODE, uponGiveMeYourReplicasMessage, GiveMeYourReplicasMessage.serializer);
        registerMessageHandler(TakeMyReplicasMessage.MSG_CODE, uponTakeMyReplicasMessage, TakeMyReplicasMessage.serializer);
        registerMessageHandler(RequestForOrderMessage.MSG_CODE, uponRequestForOrderMessage, RequestForOrderMessage.serializer);

    }

    @Override
    public void init(Properties properties) {
        this.topics = new HashMap<>(INITIAL_CAPACITY);
        this.multiPaxosLeader = null;
        this.waiting = new HashMap<>(64);
        this.unordered = new HashMap<>(64);
        this.r = new Random();
        this.isReplica = PropertiesUtils.getPropertyAsBool(properties, "replica");
        initMultiPaxos(properties);
    }


    private TreeSet<DecideNotification> message = new TreeSet<>();
    private int paxosInstaces = 0;
    private final ProtocolNotificationHandler uponOrderDecideNotification = (protocolNotification) -> {
        DecideNotification notification = (DecideNotification)protocolNotification;
        message.add(notification);
        
        executeOperations();
    };

    PersistentMap<String> messages = new PersistentMap<>(myself.toString());
    private void executeOperations() {
        DecideNotification notification = message.first();


        if(notification.getPaxosInstance() == paxosInstaces) {

            String topic = notification.getOperation().getTopic();
            for(String message: notification.getOperation().getMessages()){
                int seq = messages.put(topic,message);

                if(!isReplica){
                    DisseminatePubRequest disseminatePubRequest =
                            new DisseminatePubRequest(topic, message,seq);

                    disseminatePubRequest.setDestination(Scribe.PROTOCOL_ID);
                    sendRequestToProtocol(disseminatePubRequest);
                }
            }
            message.remove(notification);
        }
    }


    private final ProtocolMessageHandler uponRequestForOrderMessage = (protocolMessage) -> {
        RequestForOrderMessage message = (RequestForOrderMessage) protocolMessage;
        String topic = message.getTopic();
        List<String> messages = message.getMessages();
        List<String> unorderedList = this.unordered.getOrDefault(topic, new LinkedList<>());
        unorderedList.addAll(messages);
        requestOrdering(topic, messages);
    };
    private PersistentMap<String> ownedTopics;

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
    private final ProtocolMessageHandler uponTakeMyReplicasMessage = protocolMessage -> {
        TakeMyReplicasMessage m = (TakeMyReplicasMessage) protocolMessage;
        String topic = m.getTopic();
        Host replica = pickRandomFromMembership(m.getReplicas());
        List<String> toOrder = this.waiting.remove(topic);

        if (toOrder != null) {
            sendMessageSideChannel(new RequestForOrderMessage(topic, toOrder), replica);
        }

    };

    private final ProtocolMessageHandler uponGiveMeYourReplicasMessage = protocolMessage -> {
        GiveMeYourReplicasMessage m = (GiveMeYourReplicasMessage) protocolMessage;
        sendMessageSideChannel(new TakeMyReplicasMessage(m.getTopic(), membership), m.getFrom());
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
        sendRequestDecider(disseminateSubRequest);
    };
    /**
     * Sends a publish requests to the underlying protocol.
     */
    private ProtocolRequestHandler uponPublishRequest = (publishRequest) -> {
        PublishRequest pRequest = (PublishRequest) publishRequest;
        FindOwnerRequest request = new FindOwnerRequest(pRequest.getMessage());

        List<String> message = waiting.get(pRequest.getTopic());
        if (message == null) {
            sendRequestDecider(request);
            message = new LinkedList<>();
            waiting.put(pRequest.getTopic(), message);
        }

        message.add(pRequest.getMessage());
    };
    private ProtocolNotificationHandler uponOwnerNotification = protocolNotification -> {
        OwnerNotification notification = (OwnerNotification) protocolNotification;
        sendMessageSideChannel(new GiveMeYourReplicasMessage(notification.getTopic()),
                notification.getOwner());
    };


    private void requestOrdering(String topic, List<String> messages) {
        OrderOperation orderOp = new OrderOperation(topic, messages);
        ProposeRequest request = new ProposeRequest(orderOp);
        request.setDestination(MultiPaxos.PROTOCOL_ID);
        sendRequestToProtocol(request);
    }

    private Host pickRandomFromMembership(List<Host> membership) {
        int size = membership.size();
        int idx = r.nextInt() % size;

        return membership.get(idx);
    }

    /**
     * Triggers a notification to the client.
     *
     * @param pNotification to be delivered.
     */
    public void deliverNotification(ProtocolNotification pNotification) {
        MessageDeliver deliver = (MessageDeliver) pNotification;
        String topic = deliver.getTopic();

        if (this.topics.containsKey(topic)) {
            triggerNotification(new PBDeliver(deliver.getMessage(), topic));
        }
    }

    private void sendRequestDecider(ProtocolRequest request) {
        logger.info(String.format("%s - Sending message by Scribe", myself));
        request.setDestination(Chord.PROTOCOL_ID);

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
