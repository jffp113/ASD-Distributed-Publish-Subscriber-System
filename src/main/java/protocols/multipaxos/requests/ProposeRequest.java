package protocols.multipaxos.requests;

import babel.requestreply.ProtocolRequest;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import protocols.multipaxos.OrderOperation;

public class ProposeRequest extends ProtocolRequest {
    public static final short REQUEST_ID = 22222;

    private OrderOperation operation;

    public ProposeRequest(OrderOperation operation) {
        super(BCastRequest.REQUEST_ID);
        this.operation = operation;
    }

    public OrderOperation getOperation() {
        return operation;
    }
}
