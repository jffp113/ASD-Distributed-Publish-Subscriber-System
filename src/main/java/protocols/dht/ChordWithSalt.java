package protocols.dht;

import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import network.Host;
import network.INetwork;
import protocols.dht.messages.*;
import protocols.dht.requests.RouteRequest;
import protocols.dht.timers.FixFingersTimer;
import protocols.dht.timers.StabilizeTimer;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import utils.PropertiesUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ChordWithSalt extends GenericProtocol {

    private static final String M = "M";
    private static final String CONTACT = "CONTACT";
    public static final String STABILIZE_INIT = "stabilizeInit";
    public static final String STABILIZE_PERIOD = "stabilizePeriod";
    public static final String FIX_FINGERS_INIT = "fixFingersInit";
    public static final String FIX_FINGERS_PERIOD = "fixFingersPeriod";

    private int m;
    private int k;
    private int myId;
    private Host predecessor;
    private Host successor;
    private List<FingerEntry> fingers;

    public ChordWithSalt(String protoName, short protoID, INetwork net) throws Exception {
        super(protoName, protoID, net);

        registerRequestHandler(BCastRequest.REQUEST_ID, uponRouteRequest);

        registerTimerHandler(StabilizeTimer.TimerCode, uponStabilize);
        registerTimerHandler(FixFingersTimer.TimerCode, uponFixFingers);

        registerMessageHandler(FindSuccessorRequestMessage.MSG_CODE, uponFindSuccessorRequestMessage, FindSuccessorRequestMessage.serializer);
        registerMessageHandler(FindSuccessorResponseMessage.MSG_CODE, uponFindSuccessorResponseMessage, FindSuccessorResponseMessage.serializer);
        registerMessageHandler(FindPredecessorRequestMessage.MSG_CODE, uponFindPredecessorRequestMessage, FindPredecessorRequestMessage.serializer);
        registerMessageHandler(FindPredecessorReplyMessage.MSG_CODE, uponFindPredecessorReplyMessage, FindPredecessorReplyMessage.serializer);
        registerMessageHandler(NotifyPredecessorMessage.MSG_CODE, uponNotifyPredecessorMessage, NotifyPredecessorMessage.serializer);
        registerMessageHandler(FindFingerSuccessorReplyMessage.MSG_CODE, uponFindFingerSuccessorReplyMessage, FindFingerSuccessorReplyMessage.serializer);

    }

    @Override
    public void init(Properties properties) {
        try {
            m = PropertiesUtils.getPropertyAsInt(properties, M);
            k = (int) Math.pow(2, m);
            myId = generateId();
            fingers = new ArrayList<>(m);

            createRing();

            String[] contactSplit = PropertiesUtils.getPropertyAsString(properties, CONTACT).split(":");
            Host contact = new Host(InetAddress.getByName(contactSplit[0]), Integer.parseInt(contactSplit[1]));
            join(contact);

            setupPeriodicTimer(new StabilizeTimer(), PropertiesUtils.getPropertyAsInt(properties, STABILIZE_INIT),
                    PropertiesUtils.getPropertyAsInt(properties, STABILIZE_PERIOD));
            setupPeriodicTimer(new FixFingersTimer(0),
                    PropertiesUtils.getPropertyAsInt(properties, FIX_FINGERS_INIT),
                    PropertiesUtils.getPropertyAsInt(properties, FIX_FINGERS_PERIOD));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private void join(Host node) {
        predecessor = null;
        sendMessageSideChannel(new FindSuccessorRequestMessage(myId, myself), node);
    }

    private void createRing() {
        predecessor = null;
        successor = myself;

        for (int i = 0; i <= m; i++) {
            int begin = calculateFinger(myId, i);
            int end = calculateFinger(myId, i);
            fingers.add(i, new FingerEntry(begin, end, myId, myself));
        }
    }

    private final ProtocolMessageHandler uponFindFingerSuccessorReplyMessage = (protocolMessage) -> {
        FindFingerSuccessorReplyMessage message = (FindFingerSuccessorReplyMessage) protocolMessage;
        FingerEntry finger = fingers.get(message.getNext());
        Host successor = message.getSuccessor();
        finger.host = successor;
        finger.hostId = calculateId(successor.toString());
    };

    private final ProtocolTimerHandler uponFixFingers = (protocolTimer) -> {
        FixFingersTimer timer = (FixFingersTimer) protocolTimer;
        int next = timer.getNext() + 1;
        if (next == m) {
            next = 1;
        }
        FingerEntry finger = fingers.get(next);
        int successorToFindId = calculateFinger(myId, next);
        sendMessageSideChannel(new FindSuccessorRequestMessage(successorToFindId, myself, next)
                , finger.host);
    };

    //TODO: this is whewww make it better plsdsdsadsad
    private final ProtocolMessageHandler uponNotifyPredecessorMessage = (protocolMessage) -> {
        int candidate = calculateId(protocolMessage.getFrom().toString());

        if (predecessor == null) {
            predecessor = protocolMessage.getFrom();
        } else {
            int predecessorId = calculateId(predecessor.toString());
            if (isIdBetween(candidate, predecessorId, myId - 1)) {
                predecessor = protocolMessage.getFrom();
            }
        }

    };

    private final ProtocolMessageHandler uponFindPredecessorRequestMessage = (protocolMessage) -> {
        sendMessageSideChannel(new FindPredecessorReplyMessage(predecessor), protocolMessage.getFrom());
    };

    private final ProtocolMessageHandler uponFindPredecessorReplyMessage = (protocolMessage) -> {
        FindPredecessorReplyMessage message = (FindPredecessorReplyMessage) protocolMessage;
        Host temp = message.getPredecessor();
        int tempId = calculateId(temp.toString());
        int successorId = calculateId(successor.toString());

        if (isIdBetween(tempId, myId, successorId - 1)) {
            successor = temp;
        }

        sendMessageSideChannel(new NotifyPredecessorMessage(), successor);
    };


    private final ProtocolTimerHandler uponStabilize = (protocolTimer) -> {
        sendMessageSideChannel(new FindPredecessorRequestMessage(), successor);
    };

    private final ProtocolMessageHandler uponFindSuccessorRequestMessage = (protocolMessage) -> {
        FindSuccessorRequestMessage message = (FindSuccessorRequestMessage) protocolMessage;
        int nodeId = message.getNodeId();
        int successorId = calculateId(this.successor.toString());

        if (isIdBetween(nodeId, myId, successorId)) {
            ProtocolMessage m = message.getNext() != -1 ?
                    new FindFingerSuccessorReplyMessage(successor, message.getNext()) :
                    new FindSuccessorResponseMessage(successor);
            sendMessageSideChannel(m, message.getRequesterNode());
        } else {
            Host closestPrecedingNode = closestPrecedingNode(nodeId);
            sendMessageSideChannel(message, closestPrecedingNode);
        }
    };

    private final ProtocolMessageHandler uponFindSuccessorResponseMessage = (protocolMessage) -> {
        FindSuccessorResponseMessage message = (FindSuccessorResponseMessage) protocolMessage;
        successor = message.getSuccessor();
    };

    private Host closestPrecedingNode(int nodeId) {
        FingerEntry finger;

        for (int i = m; i <= 1; i--) {
            finger = fingers.get(i);
            if (isIdBetween(finger.start, myId, nodeId - 1)) {
                return finger.host;
            }
        }

        return myself;
    }

    //TODO: layer acima
    private final ProtocolRequestHandler uponRouteRequest = (protocolRequest) -> {
        RouteRequest request = (RouteRequest) protocolRequest;
    };

    private int generateId() {
        return Math.abs(myself.toString().hashCode() % k);
    }

    private int calculateId(String seed) {
        return Math.abs(seed.hashCode() % k);
    }

    //TODO: isto esta praticamente horrivel arranjar
    private boolean isIdBetween(int id, int n, int successor) {
        if (successor < 0) {
            successor = k - successor;
        }

        if (n > k) {
            return false;
        }
        while (true) {
            n = (n + 1) % k;
            if (n == id) {
                return true;
            }
            if (n == successor) {
                return false;
            }
        }
    }

    public int calculateFinger(int myId, int fingerIndex) {
        return (int) ((myId + Math.pow(2, fingerIndex - 1)) % k);
    }

}
