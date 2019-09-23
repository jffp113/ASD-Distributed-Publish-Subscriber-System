package protocols.publicsubscriber.requests;

import babel.requestreply.ProtocolRequest;

public class PublishRequest extends ProtocolRequest {
    public static short REQUEST_ID = 1000;
    private String topic;
    private String message;

    public PublishRequest(String topic, String message) {
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
