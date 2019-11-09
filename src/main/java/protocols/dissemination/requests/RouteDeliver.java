package protocols.dissemination.requests;

import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolReply;
import protocols.dissemination.MessageType;

public class RouteDeliver extends ProtocolReply {

    public static final short REQUEST_ID = 5249;

    private final ProtocolMessage messageDeliver;

    public RouteDeliver(ProtocolMessage messageDeliver) {
        super(REQUEST_ID);
        this.messageDeliver = messageDeliver;
    }

    public ProtocolMessage getMessageDeliver() {
        return messageDeliver;
    }

}
