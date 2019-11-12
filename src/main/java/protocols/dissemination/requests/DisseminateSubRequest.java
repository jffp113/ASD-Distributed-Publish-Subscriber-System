package protocols.dissemination.requests;

import babel.requestreply.ProtocolRequest;

public class DisseminateSubRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 70;

    private String topic;
    private boolean subscribe;

    public DisseminateSubRequest(String topic, boolean subscribe) {
        super(REQUEST_ID);
        this.topic = topic;
        this.subscribe = subscribe;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isSubscribe() {
        return subscribe;
    }
}
