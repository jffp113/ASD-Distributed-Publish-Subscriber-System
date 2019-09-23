package client;

import babel.Babel;
import babel.notification.INotificationConsumer;
import babel.notification.ProtocolNotification;
import network.INetwork;
import protocols.publicsubscriber.PublishSubscriber;
import protocols.publicsubscriber.requests.PublishRequest;
import protocols.publicsubscriber.requests.SubscribeRequest;
import protocols.publicsubscriber.notifications.PBDeliver;

public class Client implements INotificationConsumer {


    public static final String NETWORK_CONFIG_PROPERTIES = "network_config.properties";
    private Babel babel;
    private PublishSubscriber pubSub;

    public Client(String[] args) throws Exception {
        this.babel = Babel.getInstance();
        babel.loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        INetwork net = babel.getNetworkInstance();
        this.pubSub = new PublishSubscriber(net);
        babel.registerProtocol(pubSub);

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
