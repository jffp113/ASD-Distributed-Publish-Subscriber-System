package protocols.multipaxos.notifications;

import babel.notification.ProtocolNotification;
import protocols.multipaxos.OrderOperation;

public class DecideNotification extends ProtocolNotification {

    public static final short NOTIFICATION_ID = 4;
    public static final String NOTIFICATION_NAME = "DecideNotification";

    private OrderOperation operation;

    public DecideNotification(OrderOperation operation) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.operation = operation;
    }

    public OrderOperation getOperation() {
        return operation;
    }
}
