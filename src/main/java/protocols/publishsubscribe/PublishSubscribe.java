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
import protocols.dht.Chord;
import protocols.dht.notifications.MessageDeliver;
import protocols.dissemination.requests.DisseminateSubRequest;
import protocols.multipaxos.MultiPaxos;
import protocols.publishsubscribe.messages.GiveMeYourReplicasMessage;
import protocols.publishsubscribe.messages.TakeMyReplicasMessage;
import protocols.publishsubscribe.notifications.OwnerNotification;
import protocols.publishsubscribe.notifications.PBDeliver;
import protocols.publishsubscribe.requests.*;
import utils.PropertiesUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class PublishSubscribe extends GenericProtocol implements INotificationConsumer {
    final static Logger logger = LogManager.getLogger(PublishSubscribe.class.getName());

    public static final short PROTOCOL_ID = 1000;
    private static final int INITIAL_CAPACITY = 100;
    private static final String PROTOCOL_NAME = "Publish/Subscriber";
    private static final String MULTIPAXOS_CONTACT = "MultipaxosContact";

    private Map<String, Boolean> topics;
    private Host multiPaxosLeader;
    private Map<String, List<String>> waiting;
    private Host leader;
    private List<Host> membership;

    public PublishSubscribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        // Notifications produced
        registerNotification(PBDeliver.NOTIFICATION_ID, PBDeliver.NOTIFICATION_NAME);

        // Requests
        registerRequestHandler(PublishRequest.REQUEST_ID, uponPublishRequest);
        registerRequestHandler(SubscribeRequest.REQUEST_ID, uponSubscribeRequest);
        registerNotificationHandler(Chord.PROTOCOL_ID, OwnerNotification.NOTIFICATION_ID, uponOwnerNotification);
        registerMessageHandler(GiveMeYourReplicasMessage.NOTIFICATION_ID,uponGiveMeYourReplicasMessage,GiveMeYourReplicasMessage.serializer);
        registerMessageHandler(TakeMyReplicasMessage.NOTIFICATION_ID,uponTakeMyReplicasMessage,TakeMyReplicasMessage.serializer);
    }

    @Override
    public void init(Properties properties) {
        this.topics = new HashMap<>(INITIAL_CAPACITY);
        multiPaxosLeader = null;
        waiting = new HashMap<>(64);
        initMultiPaxos(properties);
    }

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
        if(message == null) {
            sendRequestDecider(request);
            message = new LinkedList<>();
            waiting.put(pRequest.getTopic(),message);
        }

        message.add(pRequest.getMessage());
    };

    private ProtocolNotificationHandler uponOwnerNotification = protocolNotification -> {
        OwnerNotification notification = (OwnerNotification)protocolNotification;
        sendMessageSideChannel(new GiveMeYourReplicasMessage(notification.getTopic()),
                notification.getOwner());
    };

    private final ProtocolMessageHandler uponGiveMeYourReplicasMessage = protocolMessage -> {
        GiveMeYourReplicasMessage m = (GiveMeYourReplicasMessage)protocolMessage;
        sendMessageSideChannel(new TakeMyReplicasMessage(m.getTopic(), membership),m.getFrom());
    };
    private final ProtocolMessageHandler uponTakeMyReplicasMessage = protocolMessage -> {
        TakeMyReplicasMessage m = (TakeMyReplicasMessage)protocolMessage;
        m.getReplicas().get(0); //TODO
    };

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
