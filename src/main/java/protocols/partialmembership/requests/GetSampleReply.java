package protocols.partialmembership.requests;

import babel.requestreply.ProtocolReply;
import network.Host;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class GetSampleReply extends ProtocolReply {

    public static final short REPLY_ID = GetSampleRequest.REQUEST_ID;

    private final UUID requestID;
    private final Set<Host> sample;


    public GetSampleReply(UUID requestID, Set<Host> sample) {
        super(GetSampleReply.REPLY_ID);
        this.requestID = requestID;
        this.sample = new HashSet<>(sample);
    }

    public UUID getRequestID() {
        return requestID;
    }

    public Set<Host> getSample() {
        return this.sample;
    }
}
