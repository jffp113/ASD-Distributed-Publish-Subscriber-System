package client;

import babel.Babel;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import network.INetwork;
import protocols.floadbroadcastrecovery.GossipBCast;
import protocols.floadbroadcastrecovery.notifcations.BCastDeliver;
import protocols.partialmembership.GlobalMembership;
import protocols.publishsubscribe.PublishSubscribe;
import protocols.publishsubscribe.notifications.PSDeliver;
import protocols.publishsubscribe.requests.PublishRequest;
import protocols.publishsubscribe.requests.SubscribeRequest;

import java.util.Properties;

public class Client implements INotificationConsumer {

    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";
    private static final String NOTIFICATION_FORMAT = "received event: Topic: %s Message: %s\n";
    private PublishSubscribe pubSub;

    public Client(String[] args) throws Exception {
        Babel babel = Babel.getInstance();
        Properties prop = babel.loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        INetwork net = babel.getNetworkInstance();

        GlobalMembership global = new GlobalMembership(net);
        global.init(prop);
        babel.registerProtocol(global);

        this.pubSub = new PublishSubscribe(net);
        this.pubSub.init(prop);
        babel.registerProtocol(pubSub);

        GossipBCast bCast = new GossipBCast(net);
        bCast.init(prop);
        babel.registerProtocol(bCast);

        bCast.subscribeNotification(BCastDeliver.NOTIFICATION_ID, pubSub);
        pubSub.subscribeNotification(PSDeliver.NOTIFICATION_ID, this);

        babel.start();
    }

    /**
     * Sends a subscribe request to the PublishSubscribe protocol.
     *
     * @param topic to be subscribed.
     */
    public void subscribe(String topic) {
        SubscribeRequest request = new SubscribeRequest(topic, true);
        pubSub.deliverRequest(request);
    }

    /**
     * Sends a unsubscribe request to the PublishSubscribe protocol.
     *
     * @param topic to be unsubscribed.
     */
    public void unsubscribe(String topic) {
        SubscribeRequest request = new SubscribeRequest(topic, false);
        pubSub.deliverRequest(request);
    }

    /**
     * Sends a publish request to the PublishSubscribe protocol.
     *
     * @param topic   where to publish.
     * @param message to publish.
     */
    public void publish(String topic, String message) {
        PublishRequest request = new PublishRequest(topic, message);
        pubSub.deliverRequest(request);
    }

    @Override
    public void deliverNotification(ProtocolNotification protocolNotification) {
        PSDeliver psDeliver = (PSDeliver) protocolNotification;
        System.out.printf(NOTIFICATION_FORMAT, psDeliver.getTopic(), psDeliver.getMessage());
    }
}
