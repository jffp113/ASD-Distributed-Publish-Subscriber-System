package client;

import babel.Babel;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import network.INetwork;
import protocols.dht.Chord;
import protocols.dissemination.Scribe;
import protocols.multipaxos.MultiPaxos;
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
    private static final String REPLICA = "replica";
    private PublishSubscribe pubSub;
    private Properties properties;
    private MultiPaxos paxos;
    private Babel babel;
    private INetwork net;

    public Client(String[] args) throws Exception {
        babel = Babel.getInstance();
        properties = babel.loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        net = babel.getNetworkInstance();

        if(!PropertiesUtils.getPropertyAsBool(properties, REPLICA))
            activeReplicaProtocols();

        paxos = new MultiPaxos(net);
        paxos.init(properties);
        babel.registerProtocol(paxos);

        this.pubSub = new PublishSubscribe(net);
        this.pubSub.init(properties);
        babel.registerProtocol(pubSub);

        pubSub.subscribeNotification(PBDeliver.NOTIFICATION_ID, this);

        babel.start();
    }

    private void activeReplicaProtocols() throws Exception {
        Chord chord = new Chord(net);
        chord.init(properties);
        babel.registerProtocol(chord);

        Scribe scribe = new Scribe(net);
        scribe.init(properties);
        babel.registerProtocol(scribe);

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
