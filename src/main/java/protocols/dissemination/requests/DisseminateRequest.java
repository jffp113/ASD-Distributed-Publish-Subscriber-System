package protocols.dissemination.requests;

import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolRequest;

public class DisseminateRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 69;

    private ProtocolMessage message;

    public DisseminateRequest(ProtocolMessage message) {
        super(REQUEST_ID);
        this.message = message;
    }

    public ProtocolMessage getMessage() {
        return message;
    }
}
