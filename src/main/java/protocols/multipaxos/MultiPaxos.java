package protocols.multipaxos;

import babel.exceptions.HandlerRegistrationException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import network.Host;
import network.INetwork;
import network.INodeListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.multipaxos.messages.*;
import protocols.multipaxos.notifications.DecideNotification;
import protocols.multipaxos.notifications.LeaderNotification;
import protocols.multipaxos.requests.ProposeRequest;
import protocols.multipaxos.timers.PrepareTimer;
import protocols.publishsubscribe.PublishSubscribe;
import protocols.publishsubscribe.requests.StartRequest;
import protocols.publishsubscribe.requests.StartRequestReply;
import utils.PropertiesUtils;

import java.util.*;

public class MultiPaxos extends GenericProtocol implements INodeListener {
    public static final short PROTOCOL_ID = 15243;
    private static final String PROTOCOL_NAME = "MultiPaxos";

    private static final String PREPARE_TIMEOUT = "prepareTimeout";
    private static final String KEY = "Operations";

    static Logger logger = LogManager.getLogger(MultiPaxos.class.getName());

    private int mySequenceNumber;
    private Host leader;
    private int leaderSN;
    private int prepareTimout;
    private int prepareOks;
    private boolean prepareIssued;
    private int paxosInstance;


    public MultiPaxos(INetwork net) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        registerRequestHandler(StartRequest.REQUEST_ID, uponStartRequest);

        registerNotification(DecideNotification.NOTIFICATION_ID, DecideNotification.NOTIFICATION_NAME);

        registerMessageHandler(AddReplicaMessage.MSG_CODE, uponAddReplicaMessage, AddReplicaMessage.serializer);
        registerMessageHandler(PrepareMessage.MSG_CODE, uponPrepareMessage, PrepareMessage.serializer);
        registerMessageHandler(PrepareOk.MSG_CODE, uponPrepareOk, PrepareOk.serializer);
        registerMessageHandler(ForwardProposeMessage.MSG_CODE, uponForwardProposeMessage, ForwardProposeMessage.serializer);
        registerMessageHandler(AcceptOperationMessage.MSG_CODE, uponAcceptOperation, AcceptOperationMessage.serializer);
        registerMessageHandler(AcceptOkMessage.MSG_CODE, uponAcceptOkOperation, AcceptOkMessage.serializer);
        registerTimerHandler(PrepareTimer.TIMER_CODE, uponPrepareTimer);
        registerRequestHandler(ProposeRequest.REQUEST_ID, uponProposeRequest);

    }

    @Override
    public void init(Properties properties) {
        this.leader = null;
        this.leaderSN = 0;
        this.replicaSet = new HashSet<>();
        this.pendingOperations = new HashMap<>();
        this.mySequenceNumber = 0;
        this.prepareTimout = PropertiesUtils.getPropertyAsInt(properties, PREPARE_TIMEOUT);
        this.prepareOks = 0;
        this.prepareIssued = false;
        this.paxosInstance = 0;
    }

    private final ProtocolRequestHandler uponStartRequest = (protocolRequest) -> {
        StartRequest request = (StartRequest) protocolRequest;
        Host contact = request.getContact();

        if (contact == null) {
            this.leader = myself;
            this.replicaSet.add(myself);

            StartRequestReply reply = new StartRequestReply(this.replicaSet, this.leader, this.mySequenceNumber);
            reply.setDestination(PublishSubscribe.PROTOCOL_ID);
            deliverReply(reply);
        } else {
            AddReplicaMessage message = new AddReplicaMessage(myself);
            sendMessageSideChannel(message, contact);
        }

    };

    //Paxos instace gravar a operação
    //Esperar pela maioria
    //ClIENTE OK
    private Map<Integer,OrderOperation> operationMap;

    private final ProtocolMessageHandler uponAcceptOperation = (protocolMessage) -> {
        AcceptOperationMessage message = (AcceptOperationMessage) protocolMessage;
        operationMap.put(message.getInstance(),message.getOperation());
        sendMessageToReplicaSet(message.acceptOk());
    };

    private Map<OrderOperation,Integer> operationOkAcks;

    private final ProtocolMessageHandler uponAcceptOkOperation = (protocolMessage) -> {
        AcceptOkMessage message = (AcceptOkMessage) protocolMessage;
        int instance = message.getInstance();
        OrderOperation operation = message.getOperation();
        Integer acks = operationOkAcks.getOrDefault(operation,new Integer(0))+1;
        operationOkAcks.put(operation,acks);

        if(hasMajority(acks)){
            if (instance > this.paxosInstance) {
                this.paxosInstance = instance;
//                operationMap.remove(operation); TODO
            }
            triggerNotification(new DecideNotification(operation,instance));
        }
    };

    private Set<Host> replicaSet;
    private final ProtocolRequestHandler uponProposeRequest = (protocolRequest) -> {
        ProposeRequest request = (ProposeRequest) protocolRequest;
        OrderOperation operation = request.getOperation();
        processPropose(operation);
    };


    private final ProtocolMessageHandler uponForwardProposeMessage = (protocolMessage) -> {
        ForwardProposeMessage message = (ForwardProposeMessage) protocolMessage;
        OrderOperation operation = message.getOperation();
        processPropose(operation);
    };

    private final ProtocolMessageHandler uponPrepareMessage = (protocolMessage) -> {
        PrepareMessage message = (PrepareMessage) protocolMessage;
        this.leader = message.getFrom();
        // TODO preciso de guardar o sequenceNumber do lider
        this.leaderSN = message.getSequenceNumber();

        // TODO verificar se mando as pending operations nesta camada ou na de cima
        PrepareOk prepareOk = new PrepareOk(message.getSequenceNumber(), null);
        sendMessage(prepareOk, message.getFrom());
        deliverNotification(new LeaderNotification(this.leader, this.leaderSN));
    };

    private Map<Integer, OrderOperation> pendingOperations;

    private void processPropose(OrderOperation operation) {
        if (imLeader()) {
            accept(operation);
        } else {
            sendMessage(new ForwardProposeMessage(operation), this.leader);
        }
    }

    private final ProtocolMessageHandler uponPrepareOk = (protocolMessage) -> {
        PrepareOk message = (PrepareOk) protocolMessage;

        if (this.mySequenceNumber == message.getSequenceNumber()) {
            prepareOks++;
        }

        if (hasMajority(prepareOks)) {
            Host oldLeader = this.leader;
            this.leader = myself;
            this.prepareOks = 0;
            this.prepareIssued = false;

            // TODO issue the operation to remove lider
            removeReplica(oldLeader);
        }


    };
    private final ProtocolTimerHandler uponPrepareTimer = (protocolTimer) -> {
        if (prepareIssued) {
            sendPrepare();
        }
    };

    private final ProtocolMessageHandler uponAddReplicaMessage = (protocolMessage) -> {
        AddReplicaMessage message = (AddReplicaMessage) protocolMessage;
        if (imLeader()) {
            // propose nao sei o que
            logger.info("Im leader processing request from %s\n", message.getRequester());
        } else {
            sendMessage(message, this.leader);
            logger.info("Forwarding message to leader %s\n", this.leader);
        }
    };

    private void accept(OrderOperation operation) {
        sendMessageToReplicaSet(new AcceptOperationMessage(++this.paxosInstance, operation));
    }

    private void removeReplica(Host leader) {

    }

    private OrderOperation pickHighestOperation(List<OrderOperation> operations) {
        Map<OrderOperation, Integer> result = new HashMap<>();
        int hSeq = Integer.MIN_VALUE;

        for (OrderOperation op : operations) {
            Integer occ = result.get(op);
            if (occ == null) {
                occ = new Integer(0);
            }
            occ++;
            result.put(op, occ);
            if (occ > hSeq) {
                hSeq = occ;
            }
            if ((operations.size() / 2) + 1 <= occ) {
                return op;
            }
        }
        OrderOperation o = operations.get(0);
        // o.setSeq(hSeq);
        return o;
    }

    private boolean imLeader() {
        return myself.equals(this.leader);
    }

    private void sendPrepare() {
        this.prepareIssued = true;
        this.mySequenceNumber = getNextSequenceNumber();
        logger.info("[%s] sending prepare with sequence number %s\n", myself, this.mySequenceNumber);
        setupTimer(new PrepareTimer(this.mySequenceNumber), prepareTimout);
        this.prepareOks = 0;
        sendMessageToReplicaSet(new PrepareMessage(this.mySequenceNumber));
    }

    private int getNextSequenceNumber() {
        int seqNum = this.mySequenceNumber;
        while (seqNum < leaderSN) {
            seqNum += replicaSet.size();
        }

        return seqNum;
    }

    private void sendMessageToReplicaSet(ProtocolMessage message) {
        for (Host replica : replicaSet) {
            sendMessage(message, replica);
        }
    }

    private boolean hasMajority(int acks) {
        int replicaSize = this.replicaSet.size();
        if (replicaSize <= 2) {
            return acks == replicaSize;
        }

        return acks > (replicaSize / 2) + 1;
    }

    @Override
    public void nodeDown(Host host) {
        if (host.equals(leader) && amINextLeader()) {
            logger.info("[%s] becoming the new paxos leader\n", myself);
            sendPrepare();
        }
    }

    private boolean amINextLeader() {
        Set<Host> set = new HashSet<>(replicaSet);
        set.remove(leader);

        for (Host h : set) {
            if (myself.compareTo(h) < 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void nodeUp(Host host) {
        // nothing to do here
    }

    @Override
    public void nodeConnectionReestablished(Host host) {
        // nothing to do here
    }

}
