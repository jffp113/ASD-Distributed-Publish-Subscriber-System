package protocols.publishsubscribe.notifications;

import babel.notification.ProtocolNotification;
import babel.protocol.event.ProtocolMessage;
import network.Host;

public class OwnerNotification extends ProtocolNotification {
    public static final short NOTIFICATION_ID = 2045;
    public static final String NOTIFICATION_NAME = "RouteDelivery";

    private String topic;
    private Host owner;

    public OwnerNotification(String topic, Host owner) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.topic = topic;
        this.owner = owner;
    }

    public Host getOwner() {
        return owner;
    }

    public String getTopic() {
        return topic;
    }
}
