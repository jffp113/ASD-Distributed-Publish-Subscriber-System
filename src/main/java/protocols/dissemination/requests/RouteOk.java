package protocols.dissemination.requests;

import babel.requestreply.ProtocolReply;
import network.Host;

public class RouteOk extends ProtocolReply {

    public static final short REPLY_ID = 5249;

    private final String topic;
    private final Host forwardedTo;

    public RouteOk(String topic, Host forwardedTo) {
        super(REPLY_ID);
        this.topic = topic;
        this.forwardedTo = forwardedTo;
    }

    public String getTopic() {
        return topic;
    }

    public Host getForwardedTo() {
        return forwardedTo;
    }
}
