package protocols.dht.requests;

import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolRequest;

public class RouteRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 22254;

    private final ProtocolMessage messageToRoute;

    public RouteRequest(ProtocolMessage messageToRoute) {
        super(REQUEST_ID);
        this.messageToRoute = messageToRoute;
    }

    public ProtocolMessage getMessageToRoute() {
        return messageToRoute;
    }
}
