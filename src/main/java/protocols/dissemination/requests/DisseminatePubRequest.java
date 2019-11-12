package protocols.dissemination.requests;

import babel.requestreply.ProtocolRequest;

public class DisseminatePubRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 69;

    private String topic;
    private String message;


    public DisseminatePubRequest(String topic, String message) {
        super(REQUEST_ID);
        this.topic = topic;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public String getTopic() {
        return topic;
    }
}
