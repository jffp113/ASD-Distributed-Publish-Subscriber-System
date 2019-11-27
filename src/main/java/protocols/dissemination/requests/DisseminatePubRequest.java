package protocols.dissemination.requests;

import protocols.floadbroadcastrecovery.requests.BCastRequest;

public class DisseminatePubRequest extends BCastRequest {

    private int seq;

    public DisseminatePubRequest(String topic, String message, int seq) {
        super(message,topic);
        this.seq = seq;
    }

    public int getSeq() {
        return seq;
    }
}
