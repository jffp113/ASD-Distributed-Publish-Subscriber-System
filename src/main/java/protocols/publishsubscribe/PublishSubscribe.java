package protocols.publishsubscribe;

import babel.handlers.ProtocolRequestHandler;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import babel.protocol.GenericProtocol;
import network.INetwork;
import protocols.floadbroadcastrecovery.GossipBCast;
import protocols.floadbroadcastrecovery.notifcations.BCastDeliver;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import protocols.publishsubscribe.notifications.PSDeliver;
import protocols.publishsubscribe.requests.PublishRequest;
import protocols.publishsubscribe.requests.SubscribeRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PublishSubscribe extends GenericProtocol implements INotificationConsumer {

    public final static short PROTOCOL_ID = 1000;
    public final static String PROTOCOL_NAME = "Publish/Subscriber";
    private static final int INITIAL_CAPACITY = 100;
    private Map<String, Boolean> topics;
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

    };

    @Override
    public void init(Properties properties) {
        this.topics = new HashMap<>(INITIAL_CAPACITY);
    }
    /**
     * Sends a publish request to the underlying protocol.
     */
    private ProtocolRequestHandler uponPublishRequest = (publishRequest) -> {
        PublishRequest pRequest = (PublishRequest) publishRequest;
        BCastRequest bCastRequest = new BCastRequest(pRequest.getMessage(), pRequest.getTopic());
        bCastRequest.setDestination(GossipBCast.PROTOCOL_ID);
        try {
            this.sendRequest(bCastRequest);
        } catch (Exception e) {
            // ignored
        }
    };

    public PublishSubscribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        // Notifications produced
        registerNotification(PSDeliver.NOTIFICATION_ID, PSDeliver.NOTIFICATION_NAME);

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
        BCastDeliver bcDeliver = (BCastDeliver) pNotification;
        String topic = bcDeliver.getTopic();

        if (this.topics.containsKey(topic)) {
            triggerNotification(new PSDeliver(bcDeliver.getMessage(), topic));
        }
    }
}
