package protocols.publishsubscribe.requests;

import babel.notification.ProtocolNotification;
import network.Host;

import java.util.List;

public class StartRequestNotification extends ProtocolNotification {

    public static final short NOTIFICATION_ID = StartRequest.REQUEST_ID;
    public static final String NOTIFICATION_NAME = "StartRequestReply";

    private List<Host> membership;
    private Host leader;
    private int leaderSN;
    private int paxosInstance;

    public StartRequestNotification(List<Host> membership, Host leader, int leaderSN, int paxosInstance) {
        super(NOTIFICATION_ID, NOTIFICATION_NAME);
        this.membership = membership;
        this.leader = leader;
        this.leaderSN = leaderSN;
        this.paxosInstance = paxosInstance;
    }

    public List<Host> getMembership() {
        return membership;
    }

    public Host getLeader() {
        return leader;
    }

    public int getleaderSN() {
        return leaderSN;
    }

    public int getPaxosInstance() {
        return paxosInstance;
    }
}
