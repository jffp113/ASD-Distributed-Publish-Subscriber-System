package protocols.publishsubscribe.requests;

import babel.requestreply.ProtocolRequest;
import protocols.dissemination.message.ScribeMessage;

public class FindOwnerRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 22254;

    private final String topic;

    public FindOwnerRequest(String topic) {
        super(REQUEST_ID);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return topic;
    }
}
