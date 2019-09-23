package protocols.publicsubscriber.messages;

import babel.requestreply.ProtocolRequest;

public class PublishRequest extends ProtocolRequest {
    public static short REQUEST_ID = 1;
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
