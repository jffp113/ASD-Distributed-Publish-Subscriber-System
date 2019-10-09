package protocols.floadbroadcastrecovery.requests;

import babel.requestreply.ProtocolRequest;

public class BCastRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 201;
    private static final String PAYLOAD_SEPARATOR = "|";

    private String message;
    private String topic;

    public BCastRequest(String message, String topic) {
        super(BCastRequest.REQUEST_ID);
        this.message = message;
        this.topic = topic;
    }

    public String getMessage() {
        return message;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return (topic.length() + PAYLOAD_SEPARATOR + message.length() + PAYLOAD_SEPARATOR + topic + message).getBytes();
    }
}
