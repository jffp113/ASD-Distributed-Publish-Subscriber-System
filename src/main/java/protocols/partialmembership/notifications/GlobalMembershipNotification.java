package protocols.partialmembership.notifications;

import babel.notification.ProtocolNotification;
import network.Host;

import java.util.HashSet;
import java.util.Set;

public class GlobalMembershipNotification extends ProtocolNotification {
    public final static short NOTIFICATION_ID = 101;
    public final static String NOTIFICATION_NAME = "membership";

    public final Set<Host> membership;

    public GlobalMembershipNotification(Set<Host> m) {
        super(GlobalMembershipNotification.NOTIFICATION_ID, GlobalMembershipNotification.NOTIFICATION_NAME);
        this.membership = new HashSet<>(m);
    }

    public Set<Host> getMembership() {
        return membership;
    }

    @Override
    public String toString() {
        return "GlobalMembershipNotification{" +
                "membership=" + membership +
                '}';
    }
}
