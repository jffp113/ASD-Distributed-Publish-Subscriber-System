package protocols.publishsubscribe.requests;

import babel.requestreply.ProtocolRequest;

public class SubscribeRequest extends ProtocolRequest {
    public static short REQUEST_ID =2;

    private String topic;
    private boolean subscribe;

    public SubscribeRequest(String topic, boolean subscribe) {
        super(REQUEST_ID);
        this.topic=topic;
        this.subscribe=subscribe;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isSubscribe() {
        return subscribe;
    }
}
