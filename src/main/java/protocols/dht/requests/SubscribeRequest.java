package protocols.dht.requests;

import babel.requestreply.ProtocolRequest;

public class SubscribeRequest extends ProtocolRequest {

    public static final short REQUEST_ID = 20241;

    private String topic;
    private boolean isSubscribe;


    public SubscribeRequest(String topic,boolean isSubscribe) {
        super(SubscribeRequest.REQUEST_ID);
        this.topic = topic;
        this.isSubscribe = isSubscribe;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isSubscribe() {
        return isSubscribe;
    }
}
