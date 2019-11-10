package protocols.publishsubscribe;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolRequestHandler;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import babel.protocol.GenericProtocol;
import network.INetwork;
import persistence.PersistentMap;
import protocols.dht.Chord;
import protocols.dht.messagesTopics.DisseminateRequest;
import protocols.dht.notifications.MessageDeliver;
import protocols.publishsubscribe.notifications.PBDeliver;
import protocols.publishsubscribe.requests.PublishRequest;
import protocols.publishsubscribe.requests.SubscribeRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PublishSubscribe extends GenericProtocol implements INotificationConsumer {

    public final static short PROTOCOL_ID = 1000;
    public final static String PROTOCOL_NAME = "Publish/Subscriber";
    private static final int INITIAL_CAPACITY = 100;
    private static final String TOPICS_FILE_NAME = "./pers/Topics";
    private static final String LISTEN_BASE_PORT = "listen_base_port";
    private Map<String, Boolean> topics;
    /**
     * Fill the map with the client's subscribed topics or remove them.
     */
    private ProtocolRequestHandler uponSubscribeRequest = (protocolRequest) -> {
        SubscribeRequest subscribeRequest = (SubscribeRequest) protocolRequest;
        String topic = subscribeRequest.getTopic();

        boolean isSubscribe = subscribeRequest.isSubscribe();
        protocols.dht.requests.SubscribeRequest subscribeReq =
                new protocols.dht.requests.SubscribeRequest(subscribeRequest.getTopic(), isSubscribe);
        subscribeReq.setDestination(Chord.PROTOCOL_ID);

        try {
            sendRequest(subscribeReq);
        } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
            // Ignored - should not happen.
        }

        if (this.topics.get(topic) == null) {
            if (isSubscribe) {
                this.topics.put(topic, true);
            }
        } else {
            if (!isSubscribe) {
                this.topics.remove(topic);
            }
        }

    };

    @Override
    public void init(Properties properties) {
        try {
            this.topics = new PersistentMap<>(new HashMap<>(INITIAL_CAPACITY)
                    , TOPICS_FILE_NAME + properties.getProperty(LISTEN_BASE_PORT));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends a publish requests to the underlying protocol.
     */
    private ProtocolRequestHandler uponPublishRequest = (publishRequest) -> {
        PublishRequest pRequest = (PublishRequest) publishRequest;

        DisseminateRequest disseminateRequest = new DisseminateRequest(pRequest.getTopic(), pRequest.getMessage());
        disseminateRequest.setDestination(Chord.PROTOCOL_ID);

        try {
            this.sendRequest(disseminateRequest);
        } catch (Exception e) {
            // ignored
        }
    };

    public PublishSubscribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        // Notifications produced
        registerNotification(PBDeliver.NOTIFICATION_ID, PBDeliver.NOTIFICATION_NAME);

        // Requests
        registerRequestHandler(PublishRequest.REQUEST_ID, uponPublishRequest);
        registerRequestHandler(SubscribeRequest.REQUEST_ID, uponSubscribeRequest);
    }

    /**
     * Triggers a notification to the client.
     *
     * @param pNotification to be delivered.
     */
    public void deliverNotification(ProtocolNotification pNotification) {
        //BCastDeliver bcDeliver = (BCastDeliver) pNotification;
        //String topic = bcDeliver.getTopic();

        MessageDeliver deliver = (MessageDeliver) pNotification;
        String topic = deliver.getTopic();

        if (this.topics.containsKey(topic)) {
            System.out.println(deliver.getMessage()+" "+topic);
                triggerNotification(new PBDeliver(deliver.getMessage(), topic));
        }
    }
}
