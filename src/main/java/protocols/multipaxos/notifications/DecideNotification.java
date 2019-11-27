package protocols.multipaxos.notifications;

import babel.notification.ProtocolNotification;
import protocols.multipaxos.OrderOperation;

public class DecideNotification extends ProtocolNotification implements Comparable{

    public static final short NOTIFICATION_ID = 4;
    public static final String NOTIFICATION_NAME = "DecideNotification";

    private OrderOperation operation;
    private int paxosInstance;

    public DecideNotification(OrderOperation operation, int paxosInstance) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.operation = operation;
        this.paxosInstance = paxosInstance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DecideNotification that = (DecideNotification) o;
        return paxosInstance == that.paxosInstance;
    }

    @Override
    public int hashCode() {
        return new Integer(paxosInstance).hashCode();
    }

    public OrderOperation getOperation() {
        return operation;
    }

    public int getPaxosInstance() {
        return paxosInstance;
    }

    @Override
    public int compareTo(Object o) {
        return new Integer(paxosInstance).compareTo((Integer) o);
    }
}
