package protocols.publishsubscribe;

import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.handlers.ProtocolRequestHandler;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import babel.protocol.GenericProtocol;
import network.INetwork;
import persistence.PersistentMap;
import protocols.dht.Chord;
import protocols.dht.notifications.MessageDeliver;
import protocols.dissemination.Scribe;
import protocols.dissemination.message.DeliverMessage;
import protocols.dissemination.requests.DisseminateRequest;
import protocols.publishsubscribe.notifications.PBDeliver;
import protocols.publishsubscribe.requests.PublishRequest;
import protocols.publishsubscribe.requests.SubscribeRequest;

import java.util.Properties;

public class PublishSubscribe extends GenericProtocol implements INotificationConsumer {

    public final static short PROTOCOL_ID = 1000;
    public final static String PROTOCOL_NAME = "Publish/Subscriber";

    /**
     * Fill the map with the client's subscribed topics or remove them.
     */
    private ProtocolRequestHandler uponSubscribeRequest = (protocolRequest) -> {
        SubscribeRequest subscribeRequest = (SubscribeRequest) protocolRequest;
        String topic = subscribeRequest.getTopic();
        boolean isSubscribe = subscribeRequest.isSubscribe();

        DeliverMessage message = new DeliverMessage(topic, isSubscribe, myself);
        message.setHost(myself);

        DisseminateRequest disseminateRequest =
                new DisseminateRequest(message);
        disseminateRequest.setDestination(Scribe.PROTOCOL_ID);

        try {
            sendRequest(disseminateRequest);
        } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
            // Ignored - should not happen.
        }

    };

    @Override
    public void init(Properties properties) {
    }

    /**
     * Sends a publish requests to the underlying protocol.
     */
    private ProtocolRequestHandler uponPublishRequest = (publishRequest) -> {
        PublishRequest pRequest = (PublishRequest) publishRequest;

        DeliverMessage message = new DeliverMessage(pRequest.getTopic(), pRequest.getMessage());

        DisseminateRequest disseminateRequest =
                new DisseminateRequest(message);
        disseminateRequest.setDestination(Scribe.PROTOCOL_ID);

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
        MessageDeliver deliver = (MessageDeliver) pNotification;
        String topic = deliver.getTopic();
        triggerNotification(new PBDeliver(deliver.getMessage(), topic));
    }
}
