package client;

import babel.Babel;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import network.INetwork;
import protocols.floadbroadcastrecovery.GossipBCast;
import protocols.floadbroadcastrecovery.notifcations.BCastDeliver;
import protocols.partialmembership.GlobalMembership;
import protocols.publicsubscriber.PublishSubscriber;
import protocols.publicsubscriber.requests.PublishRequest;
import protocols.publicsubscriber.requests.SubscribeRequest;
import protocols.publicsubscriber.notifications.PBDeliver;

import java.util.Properties;

public class Client implements INotificationConsumer {


    public static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";
    private Babel babel;
    private PublishSubscriber pubSub;
    private GossipBCast bcast;
    private GlobalMembership global;

    public Client(String[] args) throws Exception {
        this.babel = Babel.getInstance();
        Properties prop=  babel.loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        INetwork net = babel.getNetworkInstance();

        this.global = new GlobalMembership(net);
        this.global.init(prop);
        babel.registerProtocol(global);



        this.pubSub = new PublishSubscriber(net);
        this.pubSub.init(prop);
        babel.registerProtocol(pubSub);

        this.bcast = new GossipBCast(net);
        this.bcast.init(prop);
        babel.registerProtocol(bcast);
        this.bcast.subscribeNotification(BCastDeliver.NOTIFICATION_ID,pubSub);



        for (Short s : pubSub.producedNotifications().values()) {
            pubSub.subscribeNotification(s, this);
        }

        babel.start();
    }

    public void subscribe(String topic) {
        SubscribeRequest request = new SubscribeRequest(topic, true);
        pubSub.deliverRequest(request);

    }

    public void unsubscribe(String topic) {
        SubscribeRequest request = new SubscribeRequest(topic, false);
        pubSub.deliverRequest(request);
    }

    public void publish(String topic, String message) {
        PublishRequest request = new PublishRequest(topic, message);
        pubSub.deliverRequest(request);
    }

    @Override
    public void deliverNotification(ProtocolNotification protocolNotification) {
        PBDeliver pbDeliver = (PBDeliver) protocolNotification;
        //formatar depois
        System.out.println(pbDeliver.getMessage());
    }

}
