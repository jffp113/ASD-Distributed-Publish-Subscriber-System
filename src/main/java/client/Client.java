package client;

import babel.Babel;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import network.INetwork;
import protocols.dht.Chord;
import protocols.dht.notifications.MessageDeliver;
import protocols.dissemination.Scribe;
import protocols.dissemination.notifications.RouteDelivery;
import protocols.publishsubscribe.PublishSubscribe;
import protocols.publishsubscribe.notifications.PBDeliver;
import protocols.publishsubscribe.requests.PublishRequest;
import protocols.publishsubscribe.requests.SubscribeRequest;
import utils.PropertiesUtils;

import java.util.Properties;

public class Client implements INotificationConsumer {

    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";
    private static final String NOTIFICATION_FORMAT = "Process %s received event at %d: Topic: %s Message: %s\n";
    private static final String LISTEN_BASE_PORT = "listen_base_port";
    private PublishSubscribe pubSub;
    private Properties properties;

    public Client(String[] args) throws Exception {
        Babel babel = Babel.getInstance();
        properties = babel.loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        PropertiesUtils.loadProperties(args);
        INetwork net = babel.getNetworkInstance();

      /*  HyParView hyParView = new HyParView(net);
        hyParView.init(properties);
        babel.registerProtocol(hyParView);*/
        Chord chord = new Chord(net);
        chord.init(properties);
        babel.registerProtocol(chord);

        Scribe scribe = new Scribe(net);
        scribe.init(properties);
        babel.registerProtocol(scribe);

        this.pubSub = new PublishSubscribe(net);
        this.pubSub.init(properties);
        babel.registerProtocol(pubSub);

     /*   GossipBCast bCast = new GossipBCast(net);
        bCast.init(properties);
        babel.registerProtocol(bCast);*/

        scribe.subscribeNotification(MessageDeliver.NOTIFICATION_ID, pubSub);
        pubSub.subscribeNotification(PBDeliver.NOTIFICATION_ID, this);

        babel.start();
    }

    /**
     * Sends a subscribe requests to the PublishSubscribe protocol.
     *
     * @param topic to be subscribed.
     */
    public void subscribe(String topic) {
        SubscribeRequest request = new SubscribeRequest(topic, true);
        pubSub.deliverRequest(request);
    }

    /**
     * Sends a unsubscribe requests to the PublishSubscribe protocol.
     *
     * @param topic to be unsubscribed.
     */
    public void unsubscribe(String topic) {
        SubscribeRequest request = new SubscribeRequest(topic, false);
        pubSub.deliverRequest(request);
    }

    /**
     * Sends a publish requests to the PublishSubscribe protocol.
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
        PBDeliver pbDeliver = (PBDeliver) protocolNotification;
        System.out.printf(NOTIFICATION_FORMAT, properties.getProperty(LISTEN_BASE_PORT), System.currentTimeMillis(), pbDeliver.getTopic(), pbDeliver.getMessage());
    }
}
