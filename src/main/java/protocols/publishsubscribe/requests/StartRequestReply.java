package protocols.publishsubscribe.requests;

import babel.requestreply.ProtocolReply;
import network.Host;

import java.util.Set;

public class StartRequestReply extends ProtocolReply {

    public static final short REPLY_ID = 5666;

    private Set<Host> membership;
    private Host leader;
    private int sequenceNumber;

    public StartRequestReply(Set<Host> membership, Host leader, int sequenceNumber) {
        super(REPLY_ID);
        this.membership = membership;
        this.leader = leader;
        this.sequenceNumber = sequenceNumber;
    }

    public Set<Host> getMembership() {
        return membership;
    }

    public Host getLeader() {
        return leader;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }
}
