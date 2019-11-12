package protocols.dht.requests;

import babel.requestreply.ProtocolRequest;
import protocols.dissemination.message.ScribeMessage;

public class RouteRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 22254;

    private final ScribeMessage messageToRoute;
    private final String topic;

    public RouteRequest(ScribeMessage messageToRoute, String topic) {
        super(REQUEST_ID);
        this.messageToRoute = messageToRoute;
        this.topic = topic;
    }

    public ScribeMessage getMessageToRoute() {
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
