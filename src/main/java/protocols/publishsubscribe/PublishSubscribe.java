package protocols.publishsubscribe;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolRequestHandler;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import babel.protocol.GenericProtocol;
import babel.requestreply.ProtocolRequest;
import network.INetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.notifications.MessageDeliver;
import protocols.dissemination.Scribe;
import protocols.dissemination.requests.DisseminatePubRequest;
import protocols.dissemination.requests.DisseminateSubRequest;
import protocols.floadbroadcastrecovery.GossipBCast;
import protocols.publishsubscribe.notifications.PBDeliver;
import protocols.publishsubscribe.requests.PublishRequest;
import protocols.publishsubscribe.requests.SubscribeRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PublishSubscribe extends GenericProtocol implements INotificationConsumer {
    final static Logger logger = LogManager.getLogger(PublishSubscribe.class.getName());

    private static final short PROTOCOL_ID = 1000;
    private static final int INITIAL_CAPACITY = 100;
    private static final String PROTOCOL_NAME = "Publish/Subscriber";

    private Map<String, Boolean> topics;

    public PublishSubscribe(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        // Notifications produced
        registerNotification(PBDeliver.NOTIFICATION_ID, PBDeliver.NOTIFICATION_NAME);

        // Requests
        registerRequestHandler(PublishRequest.REQUEST_ID, uponPublishRequest);
        registerRequestHandler(SubscribeRequest.REQUEST_ID, uponSubscribeRequest);
    }

    @Override
    public void init(Properties properties) {
        this.topics = new HashMap<>(INITIAL_CAPACITY);
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

        DisseminatePubRequest disseminatePubRequest =
                new DisseminatePubRequest(pRequest.getTopic(), pRequest.getMessage());

        sendRequestDecider(disseminatePubRequest);
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

    public void sendRequestDecider(ProtocolRequest request){
        if(isFrequentTopic()){
            logger.info(String.format("%s - Sending message by Broadcast",myself));
            request.setDestination(GossipBCast.PROTOCOL_ID);
        }else{
            logger.info(String.format("%s - Sending message by Scribe",myself));
            request.setDestination(Scribe.PROTOCOL_ID);
        }

        sendRequestToProtocol(request);
    }

    private boolean isFrequentTopic() {
        return false;
    }

    private void sendRequestToProtocol(ProtocolRequest request){
        try {
            this.sendRequest(request);
        } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
            logger.error(destinationProtocolDoesNotExist);
        }
    }

}
