package protocols.dissemination.requests;

import protocols.floadbroadcastrecovery.requests.BCastRequest;

public class DisseminatePubRequest extends BCastRequest {

    public DisseminatePubRequest(String topic, String message) {
        super(message,topic);

    }

}
