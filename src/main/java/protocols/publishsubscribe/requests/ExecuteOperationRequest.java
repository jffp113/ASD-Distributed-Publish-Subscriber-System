package protocols.publishsubscribe.requests;

import babel.requestreply.ProtocolRequest;
import protocols.multipaxos.Operation;

public class ExecuteOperationRequest extends ProtocolRequest {
    public static short REQUEST_ID = 13000;
    private int sequenceNumber;
    private Operation operation;

    public ExecuteOperationRequest(int sequenceNumber, Operation operation) {
        super(REQUEST_ID);
        this.sequenceNumber = sequenceNumber;
        this.operation = operation;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public Operation getOperation() {
        return operation;
    }
}
