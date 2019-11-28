package protocols.multipaxos.notifications;

import babel.notification.ProtocolNotification;
import protocols.multipaxos.Operation;

public class DecideNotification extends ProtocolNotification {

    public static final short NOTIFICATION_ID = 4;
    public static final String NOTIFICATION_NAME = "DecideNotification";

    private Operation operation;

    public DecideNotification(Operation operation) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.operation = operation;
    }

    public Operation getOperation() {
        return this.operation;
    }

}
