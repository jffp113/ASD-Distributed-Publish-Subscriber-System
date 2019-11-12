package protocols.dissemination.notifications;

import babel.notification.ProtocolNotification;
import babel.protocol.event.ProtocolMessage;

public class RouteDelivery extends ProtocolNotification {
    public static final short NOTIFICATION_ID = 2045;
    public static final String NOTIFICATION_NAME = "RouteDelivery";

    private ProtocolMessage message;

    public RouteDelivery(ProtocolMessage message) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.message = message;
    }

    public ProtocolMessage getMessage() {
        return message;
    }

}
