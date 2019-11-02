package protocols.dht.messagesTopics;

import babel.requestreply.ProtocolRequest;

public class DisseminateRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 123;

    private String topic;
    private String message;

    public DisseminateRequest(String topic, String message) {
        super(REQUEST_ID);
        this.topic = topic;
        this.message = message;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessage() {
        return message;
    }
}
