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
import protocols.partialmembership.timers.DebugTimer;
import protocols.publishsubscribe.requests.StartRequest;
import protocols.publishsubscribe.requests.StartRequestNotification;
import utils.PropertiesUtils;
import utils.Utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class MultiPaxos extends GenericProtocol implements INodeListener {
    public static final short PROTOCOL_ID = 15243;
    private static final String PROTOCOL_NAME = "MultiPaxos";

    private static final String PREPARE_TIMEOUT = "prepareTimeout";

    static Logger logger = LogManager.getLogger(MultiPaxos.class.getName());

    private int mySequenceNumber;
    private Host leader;
    private int leaderSN;
    private int prepareTimout;
    private int prepareOks;
    private boolean prepareIssued;
    private int paxosInstance;

    private final ProtocolMessageHandler uponPrepareMessage = (protocolMessage) -> {
        PrepareMessage message = (PrepareMessage) protocolMessage;

        this.leader = message.getFrom();
        this.leaderSN = message.getSequenceNumber();

        deliverNotification(new LeaderNotification(this.leader, this.leaderSN));

        PrepareOk prepareOk = new PrepareOk(message.getSequenceNumber());
        sendMessage(prepareOk, message.getFrom());
    };

    private Host oldLeader;
    private Map<Integer, Integer> operationOkAcks;

    private final ProtocolRequestHandler uponStartRequest = (protocolRequest) -> {
        StartRequest request = (StartRequest) protocolRequest;
        Host contact = request.getContact();

        if (contact == null) {
            addNetworkPeer(myself);
            this.leader = myself;
            this.leaderSN = 1;
            this.mySequenceNumber = 1;
            this.replicas.add(myself);

            StartRequestNotification notification = new StartRequestNotification(this.replicas, this.leader, this.mySequenceNumber, paxosInstance);
            triggerNotification(notification);
        } else {
            AddReplicaMessage message = new AddReplicaMessage(myself);
            sendMessageSideChannel(message, contact);
        }
    };

    private List<Host> replicas;
    private final ProtocolTimerHandler uponDebugTimer = (protocolTimer) -> {
        StringBuilder sb = new StringBuilder();
        sb.append("--------------------\n");
        sb.append("Leader->" + leader + "(" + leaderSN + ")" + "\n");
        sb.append(myself + "(" + mySequenceNumber + ")" + "\n");
        sb.append(replicas + "\n");

        logger.info(sb.toString());
    };

    private Host getHost(String[] contact) {
        try {
            return new Host(InetAddress.getByName(contact[0]), Integer.parseInt(contact[1]));
        } catch (UnknownHostException e) {
            // Ignored
            e.printStackTrace();
        }
        return null;
    }
    private final ProtocolMessageHandler uponAcceptOkOperation = (protocolMessage) -> {
        AcceptOkMessage message = (AcceptOkMessage) protocolMessage;
        int instance = message.getOperation().getInstance();
        Operation operation = message.getOperation();

        if (operation.getSequenceNumber() == this.leaderSN) {
            int oid = operation.getId();
            Integer acks = operationOkAcks.getOrDefault(oid, 0) + 1;
            operationOkAcks.put(oid, acks);

            if (hasMajorityAcks(acks)) {
                if (instance > this.paxosInstance) {
                    this.paxosInstance = instance;
                }
                if (operation.getType() == Operation.Type.ADD_REPLICA) {
                    processAddReplica(operation);
                } else if (operation.getType() == Operation.Type.REMOVE_REPLICA) {
                    processRemoveReplica(operation);
                }
                triggerNotification(new DecideNotification(operation));
            } else {

                if (operation.getType() == Operation.Type.REMOVE_REPLICA && this.replicas.size() == 2) {
                    processRemoveReplica(operation);
                }
            }
        }

    };

    private final ProtocolMessageHandler uponAddReplicaMessage = (protocolMessage) -> {
        AddReplicaMessage message = (AddReplicaMessage) protocolMessage;
        if (imLeader()) {
            Content content = new MembershipUpdateContent(message.getReplica());
            Operation newReplicaOp = new Operation(Utils.generateId(), ++paxosInstance, this.mySequenceNumber, Operation.Type.ADD_REPLICA, content);
            sendMessageToReplicaSet(new AcceptOperationMessage(newReplicaOp), null);
            logger.info(String.format("Im leader processing request from %s\n", message.getReplica().toString()));
        } else {
            sendMessage(message, this.leader);
            logger.info(String.format("Forwarding message to leader %s\n", this.leader.toString()));
        }
    };

    private final ProtocolMessageHandler uponAcceptOperation = (protocolMessage) -> {
        AcceptOperationMessage message = (AcceptOperationMessage) protocolMessage;
        Operation operation = message.getOperation();

        if (operation.getSequenceNumber() == this.leaderSN) {
            sendMessageToReplicaSet(new AcceptOkMessage(message.getOperation()), null);
        }

    };

    public MultiPaxos(INetwork net) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        registerRequestHandler(StartRequest.REQUEST_ID, uponStartRequest);
        registerTimerHandler(DebugTimer.TimerCode, uponDebugTimer);
        registerNotification(DecideNotification.NOTIFICATION_ID, DecideNotification.NOTIFICATION_NAME);
        registerNotification(StartRequestNotification.NOTIFICATION_ID, StartRequestNotification.NOTIFICATION_NAME);

        registerMessageHandler(AddReplicaMessage.MSG_CODE, uponAddReplicaMessage, AddReplicaMessage.serializer);
        registerMessageHandler(PrepareMessage.MSG_CODE, uponPrepareMessage, PrepareMessage.serializer);
        registerMessageHandler(PrepareOk.MSG_CODE, uponPrepareOk, PrepareOk.serializer);
        registerMessageHandler(ForwardProposeMessage.MSG_CODE, uponForwardProposeMessage, ForwardProposeMessage.serializer);
        registerMessageHandler(AcceptOperationMessage.MSG_CODE, uponAcceptOperation, AcceptOperationMessage.serializer);
        registerMessageHandler(AcceptOkMessage.MSG_CODE, uponAcceptOkOperation, AcceptOkMessage.serializer);
        registerMessageHandler(AddReplicaResponseMessage.MSG_CODE, uponAddReplicaResponseMessage, AddReplicaResponseMessage.serializer);
        registerTimerHandler(PrepareTimer.TIMER_CODE, uponPrepareTimer);
        registerRequestHandler(ProposeRequest.REQUEST_ID, uponProposeRequest);
        registerNodeListener(this);
    }

    private void sendMessageToReplicaSet(Set<Host> replicaSet, ProtocolMessage message) {
        for (Host replica : replicaSet) {
            sendMessage(message, replica);
        }
    }

    private final ProtocolMessageHandler uponAddReplicaResponseMessage = (protocolMessage) -> {
        AddReplicaResponseMessage message = (AddReplicaResponseMessage) protocolMessage;

        this.leader = message.getFrom();
        this.leaderSN = message.getLeaderSN();
        this.replicas = message.getReplicaSet();

        for (Host replica : replicas) {
            addNetworkPeer(replica);
        }

        this.paxosInstance = message.getInstance();
        this.mySequenceNumber = message.getReplicaSN();

        triggerNotification(new StartRequestNotification(replicas, leader, leaderSN, paxosInstance));
    };
    private final ProtocolMessageHandler uponPrepareOk = (protocolMessage) -> {
        PrepareOk message = (PrepareOk) protocolMessage;

        if (this.mySequenceNumber == message.getSequenceNumber()) {
            prepareOks++;
        }

        if (hasMajority(prepareOks)) {
            this.leader = myself;
            this.leaderSN = this.mySequenceNumber;
            this.prepareOks = 0;
            this.prepareIssued = false;

            Content content = new MembershipUpdateContent(oldLeader);
            Operation op = new Operation(Utils.generateId(), ++paxosInstance, this.mySequenceNumber, Operation.Type.REMOVE_REPLICA, content);
            sendMessageToReplicaSet(new AcceptOperationMessage(op), null);
        }


    };

    private final ProtocolRequestHandler uponProposeRequest = (protocolRequest) -> {
        ProposeRequest request = (ProposeRequest) protocolRequest;
        Operation operation = request.getOperation();
        processPropose(operation);
    };

    @Override
    public void init(Properties properties) {
        this.leader = null;
        this.leaderSN = 0;
        this.mySequenceNumber = 0;
        this.replicas = new LinkedList<>();
        this.operationOkAcks = new HashMap<>();
        this.prepareTimout = PropertiesUtils.getPropertyAsInt(properties, PREPARE_TIMEOUT);
        this.prepareOks = 0;
        this.prepareIssued = false;
        this.paxosInstance = 0;

        setupPeriodicTimer(new DebugTimer(), 1000, 10000);

        String rawContacts = PropertiesUtils.getPropertyAsString(properties, "multipaxos_contact");

      /*  if (rawContacts != null) {
            String[] multipaxosContact = rawContacts.split(":");
            Host contact = getHost(multipaxosContact);

            AddReplicaMessage message = new AddReplicaMessage(myself);
            sendMessageSideChannel(message, contact);
        } else {
            this.leader = myself;
            this.leaderSN = 1;
            this.mySequenceNumber = 1;
            this.replicas.add(myself);
            addNetworkPeer(myself);
        }*/
    }

    private void sendMessageToReplicaSet(ProtocolMessage message, Host except) {
        Set<Host> aux = new HashSet<>(replicas);
        aux.remove(except);
        sendMessageToReplicaSet(aux, message);
    }

    private void processPropose(Operation operation) {
        if (imLeader()) {
            accept(operation);
        } else {
            sendMessage(new ForwardProposeMessage(operation), this.leader);
        }
    }

    private void processAddReplica(Operation operation) {
        MembershipUpdateContent content = (MembershipUpdateContent) operation.getContent();
        replicas.add(content.getReplica());
        addNetworkPeer(content.getReplica());
        if (imLeader()) {
            sendMessage(new AddReplicaResponseMessage(replicas, Utils.generateSeqNum(), this.mySequenceNumber, paxosInstance), content.getReplica());
        }
    }

    private final ProtocolMessageHandler uponForwardProposeMessage = (protocolMessage) -> {
        ForwardProposeMessage message = (ForwardProposeMessage) protocolMessage;
        Operation operation = message.getOperation();
        processPropose(operation);
    };

    private void processRemoveReplica(Operation operation) {
        MembershipUpdateContent content = (MembershipUpdateContent) operation.getContent();
        replicas.remove(content.getReplica());
        removeNetworkPeer(content.getReplica());
    }

    private void accept(Operation operation) {
        Operation op = new Operation(operation.getId(), ++paxosInstance, this.mySequenceNumber, operation.getType(), operation.getContent());
        sendMessageToReplicaSet(new AcceptOperationMessage(op), null);
    }

    private boolean hasMajority(int acks) {
        return hasMajorityAcks(acks) || replicas.size() == 2;
    }

    private final ProtocolTimerHandler uponPrepareTimer = (protocolTimer) -> {
        if (prepareIssued) {
            sendPrepare();
        }
    };

    private boolean imLeader() {
        return myself.equals(this.leader);
    }

    private void sendPrepare() {
        this.prepareIssued = true;
        this.mySequenceNumber = getNextSequenceNumber();
        this.oldLeader = this.leader;
        logger.info("[%s] sending prepare with sequence number %s\n", myself, this.mySequenceNumber);
        setupTimer(new PrepareTimer(this.mySequenceNumber), prepareTimout);
        this.prepareOks = 0;
        sendMessageToReplicaSet(new PrepareMessage(this.mySequenceNumber), null);
    }

    private int getNextSequenceNumber() {
        int seqNum = this.mySequenceNumber;
        while (seqNum < leaderSN) {
            seqNum += replicas.size();
        }

        return seqNum;
    }


    private boolean hasMajorityAcks(int acks) {
        int replicaSize = this.replicas.size();
        if (replicaSize <= 2) {
            return acks == replicaSize;
        }
        return acks >= (replicaSize / 2) + 1;
    }

    private boolean amINextLeader() {
        Set<Host> set = new HashSet<>(replicas);
        set.remove(leader);

        for (Host h : set) {
            if (myself.compareTo(h) < 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void nodeDown(Host host) {
        if (imLeader()) {
            Content content = new MembershipUpdateContent(host);
            Operation op = new Operation(Utils.generateId(), ++paxosInstance, this.mySequenceNumber, Operation.Type.REMOVE_REPLICA, content);
            sendMessageToReplicaSet(new AcceptOperationMessage(op), null);
        }

        if (host.equals(leader) && amINextLeader()) {
            logger.info("[%s] becoming the new paxos leader\n", myself);
            sendPrepare();
        }
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
