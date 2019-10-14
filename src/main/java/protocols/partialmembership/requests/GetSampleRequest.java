package protocols.partialmembership.requests;

import babel.requestreply.ProtocolRequest;

import java.util.UUID;

public class GetSampleRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 101;

    private final int fanout;
    private UUID identifier;

    public GetSampleRequest(int fanout) {
        super(GetSampleRequest.REQUEST_ID);
        this.fanout = fanout;
        this.identifier = UUID.randomUUID();
    }

    public GetSampleRequest(int fanout, UUID identifier ) {
        super(GetSampleRequest.REQUEST_ID);
        this.fanout = fanout;
        this.identifier = identifier;
    }

    public int getFanout() {
        return this.fanout;
    }

    public UUID getIdentifier() {
        return identifier;
    }
}
