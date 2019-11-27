package protocols.multipaxos.notifications;

import babel.notification.ProtocolNotification;
import protocols.multipaxos.Operation;

public class DecideNotification extends ProtocolNotification implements Comparable{

    public static final short NOTIFICATION_ID = 4;
    public static final String NOTIFICATION_NAME = "DecideNotification";

    private Operation operation;

    public DecideNotification(Operation operation) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.operation = operation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DecideNotification that = (DecideNotification) o;
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
