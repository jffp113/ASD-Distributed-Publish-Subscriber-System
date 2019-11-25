package protocols.multipaxos.notifications;

import babel.notification.ProtocolNotification;
import network.Host;

public class LeaderNotification extends ProtocolNotification {

    public static final short NOTIFICATION_ID = 2;
    public static final String NOTIFICATION_NAME = "LeaderNotification";

    private Host leader;
    private int leaderSN;

    public LeaderNotification(Host leader, int leaderSN) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.leader = leader;
        this.leaderSN = leaderSN;
    }

    public Host getLeader() {
        return leader;
    }

    public int getLeaderSN() {
        return leaderSN;
    }
}
