package protocols.dht.requests;

import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolRequest;

public class RouteRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 22254;

    private final ProtocolMessage messageToRoute;
    private final String topic;

    public RouteRequest(ProtocolMessage messageToRoute,String topic) {
        super(REQUEST_ID);
        this.messageToRoute = messageToRoute;
        this.topic = topic;
    }

    public ProtocolMessage getMessageToRoute() {
        return messageToRoute;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return topic + " " + messageToRoute.toString();
    }
}
