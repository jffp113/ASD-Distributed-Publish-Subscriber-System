package protocols.publishsubscribe.requests;

import babel.requestreply.ProtocolRequest;
import protocols.multipaxos.OrderOperation;

public class ExecuteOperationRequest extends ProtocolRequest {
    public static short REQUEST_ID = 13000;
    private int sequenceNumber;
    private OrderOperation operation;

    public ExecuteOperationRequest(int sequenceNumber, OrderOperation operation) {
        super(REQUEST_ID);
        this.sequenceNumber = sequenceNumber;
        this.operation = operation;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public OrderOperation getOperation() {
        return operation;
    }
}
