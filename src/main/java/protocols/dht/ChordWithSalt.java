package protocols.dht;

import babel.exceptions.HandlerRegistrationException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import babel.timer.ProtocolTimer;
import network.Host;
import network.INetwork;
import network.INodeListener;
import protocols.dht.messages.*;
import protocols.dht.messagesTopics.DeliverMessage;
import protocols.dht.messagesTopics.DisseminateRequest;
import protocols.dht.messagesTopics.ForwardDisseminateMessage;
import protocols.dht.messagesTopics.ForwardSunscribeMessage;
import protocols.dht.notifications.MessageDeliver;
import protocols.dht.requests.RouteRequest;
import protocols.dht.requests.SubscribeRequest;
import protocols.dht.timers.FixFingersTimer;
import protocols.dht.timers.StabilizeTimer;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import protocols.partialmembership.timers.DebugTimer;
import utils.PropertiesUtils;

import java.net.InetAddress;
import java.util.*;

public class ChordWithSalt extends GenericProtocol implements INodeListener {

    public static final short PROTOCOL_ID = 1243;
    public static final String PROTOCOL_NAME = "ChordWithSalt";

    private static final String M = "m";
    private static final String CONTACT = "Contact";
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
    private int next;

    public ChordWithSalt(INetwork net) throws Exception {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);

        registerNotification(MessageDeliver.NOTIFICATION_ID, MessageDeliver.NOTIFICATION_NAME);

        registerRequestHandler(BCastRequest.REQUEST_ID, uponRouteRequest);

        registerTimerHandler(StabilizeTimer.TimerCode, uponStabilize);
        registerTimerHandler(FixFingersTimer.TimerCode, uponFixFingers);
        registerTimerHandler(DebugTimer.TimerCode, uponDebugTimer);

        registerMessageHandler(FindSuccessorRequestMessage.MSG_CODE, uponFindSuccessorRequestMessage, FindSuccessorRequestMessage.serializer);
        registerMessageHandler(FindSuccessorResponseMessage.MSG_CODE, uponFindSuccessorResponseMessage, FindSuccessorResponseMessage.serializer);
        registerMessageHandler(FindPredecessorRequestMessage.MSG_CODE, uponFindPredecessorRequestMessage, FindPredecessorRequestMessage.serializer);
        registerMessageHandler(FindPredecessorReplyMessage.MSG_CODE, uponFindPredecessorReplyMessage, FindPredecessorReplyMessage.serializer);
        registerMessageHandler(NotifyPredecessorMessage.MSG_CODE, uponNotifyPredecessorMessage, NotifyPredecessorMessage.serializer);
        registerMessageHandler(FindFingerSuccessorReplyMessage.MSG_CODE, uponFindFingerSuccessorReplyMessage, FindFingerSuccessorReplyMessage.serializer);
        registerMessageHandler(FindFingerSuccessorRequestMessage.MSG_CODE, uponFindFingerSuccessorRequestMessage, FindFingerSuccessorRequestMessage.serializer);

        registerRequestHandler(DisseminateRequest.REQUEST_ID, uponDisseminateRequest);
        registerMessageHandler(ForwardDisseminateMessage.MSG_CODE, uponForwardDisseminateMessage, ForwardDisseminateMessage.serializer);

        registerNodeListener(this);
    }

    private final ProtocolTimerHandler uponDebugTimer = (protocolTimer) -> {
        System.out.println("Debug:");
        System.out.println(myId);
        System.out.println("Predecessor: " + predecessor);
        System.out.println("Successor: " + successor);
        for (FingerEntry f : fingers) {
            System.out.println(f);
        }
    };

    @Override
    public void init(Properties properties) {
        try {
            constructorManager();
            m = PropertiesUtils.getPropertyAsInt(properties, M);
            k = (int) Math.pow(2, m);
            myId = generateId();
            fingers = new ArrayList<>(m);
            this.next = 0;
            String contactString = PropertiesUtils.getPropertyAsString(properties, CONTACT);
            createRing();
            if (PropertiesUtils.getPropertyAsBool(properties, "debug")) {
                setupPeriodicTimer(new DebugTimer(), 1000, 3000);
            }

            if (contactString != null) {
                String[] contactSplit = contactString.split(":");
                Host contact = new Host(InetAddress.getByName(contactSplit[0]), Integer.parseInt(contactSplit[1]));
                join(contact);
            }


            setupPeriodicTimer(new StabilizeTimer(), PropertiesUtils.getPropertyAsInt(properties, STABILIZE_INIT),
                    PropertiesUtils.getPropertyAsInt(properties, STABILIZE_PERIOD));
            setupPeriodicTimer(new FixFingersTimer(),
                    PropertiesUtils.getPropertyAsInt(properties, FIX_FINGERS_INIT),
                    PropertiesUtils.getPropertyAsInt(properties, FIX_FINGERS_PERIOD));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void join(Host node) {
        sendMessageSideChannel(new FindSuccessorRequestMessage(myId, myself), node);
    }

    private void createRing() {
        predecessor = null;

        for (int i = 0; i < m; i++) {
            int begin = calculateFinger(myId, i);
            fingers.add(i, new FingerEntry(begin, myId, myself));
        }

        changeSuccessor(myself);
    }

    private final ProtocolMessageHandler uponFindFingerSuccessorRequestMessage = (protocolMessage) -> {
        FindFingerSuccessorRequestMessage message = (FindFingerSuccessorRequestMessage) protocolMessage;
        int nodeId = message.getNodeId();
        int successorId = calculateId(this.successor.toString());

        if (isIdBetween(nodeId, myId, successorId, true)) {
            sendMessageSideChannel(new FindFingerSuccessorReplyMessage(successor, message.getNext()), message.getRequesterNode());
        } else {
            Host closestPrecedingNode = closestPrecedingNode(nodeId);
            if (!closestPrecedingNode.equals(myself))
                sendMessageSideChannel(message, closestPrecedingNode);
        }
    };

    private final ProtocolMessageHandler uponFindFingerSuccessorReplyMessage = (protocolMessage) -> {
        FindFingerSuccessorReplyMessage message = (FindFingerSuccessorReplyMessage) protocolMessage;
        FingerEntry finger = fingers.get(message.getNext());
        Host successor = message.getSuccessor();
        finger.host = successor;
        finger.hostId = calculateId(successor.toString());
    };

    private final ProtocolTimerHandler uponFixFingers = (protocolTimer) -> {
        if (++next == m) {
            next = 1;
        }

        FingerEntry finger = fingers.get(next);
        int successorToFindId = calculateFinger(myId, next);
        sendMessageSideChannel(new FindFingerSuccessorRequestMessage(successorToFindId, myself, next), successor);
    };

    //TODO: this is whewww make it better plsdsdsadsad
    private final ProtocolMessageHandler uponNotifyPredecessorMessage = (protocolMessage) -> {
        int candidate = calculateId(protocolMessage.getFrom().toString());

        if (predecessor == null) {
            addNetworkPeer(protocolMessage.getFrom());
            predecessor = protocolMessage.getFrom();
        } else {
            int predecessorId = calculateId(predecessor.toString());
            if (isIdBetween(candidate, predecessorId, myId, false)) {

                removePredecessorNetworkPeer();
                addNetworkPeer(protocolMessage.getFrom());
                predecessor = protocolMessage.getFrom();
            }
        }

    };

    private final ProtocolMessageHandler uponFindPredecessorRequestMessage = (protocolMessage) -> {
        Host predecessorToSend = predecessor;
        if (predecessorToSend == null) {
            predecessorToSend = myself;
        }
        sendMessageSideChannel(new FindPredecessorReplyMessage(predecessorToSend), protocolMessage.getFrom());
    };

    private final ProtocolMessageHandler uponFindPredecessorReplyMessage = (protocolMessage) -> {

        FindPredecessorReplyMessage message = (FindPredecessorReplyMessage) protocolMessage;
        Host temp = message.getPredecessor();
        if (temp != null) {
            int tempId = calculateId(temp.toString());
            int successorId = calculateId(successor.toString());

            if (successorId == myId || isIdBetween(tempId, myId, successorId, false)) {
                changeSuccessor(temp);

            }
        }

        if (!successor.equals(myself))
            sendMessageSideChannel(new NotifyPredecessorMessage(), successor);
    };


    private final ProtocolTimerHandler uponStabilize = (protocolTimer) -> {
        sendMessageSideChannel(new FindPredecessorRequestMessage(), successor);
    };

    private final ProtocolMessageHandler uponFindSuccessorRequestMessage = (protocolMessage) -> {
        FindSuccessorRequestMessage message = (FindSuccessorRequestMessage) protocolMessage;
        int nodeId = message.getNodeId();
        int successorId = calculateId(this.successor.toString());

        if (isIdBetween(nodeId, myId, successorId, true)) {
            sendMessageSideChannel(new FindSuccessorResponseMessage(successor), message.getRequesterNode());
        } else {
            Host closestPrecedingNode = closestPrecedingNode(nodeId);
            if (!closestPrecedingNode.equals(myself))
                sendMessageSideChannel(message, closestPrecedingNode);
            else
                sendMessageSideChannel(new FindSuccessorResponseMessage(myself), message.getRequesterNode());
        }
    };

    private final ProtocolMessageHandler uponFindSuccessorResponseMessage = (protocolMessage) -> {
        FindSuccessorResponseMessage message = (FindSuccessorResponseMessage) protocolMessage;
        changeSuccessor(message.getSuccessor());
    };

    private Host closestPrecedingNode(int nodeId) {
        FingerEntry finger;

        for (int i = m - 1; i >= 0; i--) {
            finger = fingers.get(i);
            if (isIdBetween(finger.start, myId, nodeId, false)) {
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

    private boolean isIdBetween(int id, int start, int end, boolean includeEnd) {
        int minLimit = start;
        int maxLimit = end;

        if (minLimit > maxLimit) {
            int amountToMaxLimit = Math.abs(k - id);
            if (amountToMaxLimit < id) {
                maxLimit = k;
            } else {
                minLimit = -1;
            }
        }

        return includeEnd ? id > minLimit && id <= maxLimit : id > minLimit && id < maxLimit;
    }

    public int calculateFinger(int myId, int fingerIndex) {
        return (int) ((myId + Math.pow(2, fingerIndex)) % k);
    }

    private void sendMsgIfNotMe(ProtocolMessage msg, Host to) {
        if (!to.equals(myself))
            sendMessageSideChannel(msg, to);
    }

    private void changeSuccessor(Host newSuccessor) {
        System.err.println("Change Sucessor: " + newSuccessor);
        int successorId = calculateId(newSuccessor.toString());

        FingerEntry finger = fingers.get(0);
        finger.host = newSuccessor;
        finger.hostId = successorId;

        if (successor != null) {
            removeSuccessorNetworkPeer();
        }
        successor = newSuccessor;
        addNetworkPeer(successor);
    }

    @Override
    public void nodeDown(Host host) {
        if (predecessor.equals(host)) {
            removePredecessorNetworkPeer();
            predecessor = null;
        }

        if (successor.equals(host)) {
            changeSuccessor(myself);
        }
    }

    @Override
    public void nodeUp(Host host) {
    }

    @Override
    public void nodeConnectionReestablished(Host host) {
    }

    private void removeSuccessorNetworkPeer() {
        if (!successor.equals(predecessor)) {
            removeNetworkPeer(successor);
        }
    }

    private void removePredecessorNetworkPeer() {
        if (!predecessor.equals(successor)) {
            removeNetworkPeer(predecessor);
        }
    }

    // Topic Manager //TODO no crahes
    private Map<String, Set<Host>> topics;

    private void constructorManager() throws HandlerRegistrationException {
        topics = new HashMap<>();
        initManager();
    }

    private void initManager() throws HandlerRegistrationException {
        registerRequestHandler(SubscribeRequest.REQUEST_ID, uponSubscribeRequest);
        registerMessageHandler(ForwardSunscribeMessage.MSG_CODE, uponForwardSunscribeMessage, ForwardSunscribeMessage.serializer);
        registerMessageHandler(DeliverMessage.MSG_CODE, uponDeliverMessage, DeliverMessage.serializer);
    }

    //Request Subscribe from level up
    private ProtocolRequestHandler uponSubscribeRequest = (protocolRequest) -> {
        SubscribeRequest request = (SubscribeRequest) protocolRequest;
        subscribeOrUnsubscribe(myself, request.getTopic(), request.isSubscribe());
    };

    private void subscribeOrUnsubscribe(Host host, String topic, boolean isSubscribe) {
        int topicId = calculateId(topic);
        Host closestPrecedingNode = closestPrecedingNode(topicId);
        boolean forMe = myself.equals(closestPrecedingNode);
        if (forMe) {
            Set<Host> hosts = topics.get(topic);
            if (hosts == null)
                hosts = new HashSet<>();

            if (isSubscribe)
                hosts.add(host);
            else
                hosts.remove(host);

            topics.put(topic, hosts);
        } else {
            sendMessageIfNotMe(new ForwardSunscribeMessage(topic, host, isSubscribe), closestPrecedingNode);
        }
    }

    //Request Subscribe from other ps
    private ProtocolMessageHandler uponForwardSunscribeMessage = protocolMessage -> {
        ForwardSunscribeMessage message = (ForwardSunscribeMessage) protocolMessage;
        subscribeOrUnsubscribe(message.getHost(), message.getTopic(), message.isSubscribe());
    };

    //Request to disseminate
    private ProtocolRequestHandler uponDisseminateRequest = (protocolRequest) -> {
        DisseminateRequest request = (DisseminateRequest) protocolRequest;
        disseminate(request.getTopic(), request.getMessage());
    };

    private void disseminate(String topic, String message) {
        int topicId = calculateId(topic);
        Host closestPrecedingNode = closestPrecedingNode(topicId);
        boolean forMe = myself.equals(closestPrecedingNode);
        if (forMe) {
            triggerNotification(new MessageDeliver(topic, message));
            for (Host subscriber : topics.get(topic)) {
                sendMessageIfNotMe(new DeliverMessage(topic, message), subscriber);
            }
        } else {
            sendMessageIfNotMe(new ForwardDisseminateMessage(topic, message), closestPrecedingNode);
        }
    }

    ProtocolMessageHandler uponForwardDisseminateMessage = (protocolMessage) -> {
        ForwardDisseminateMessage message = (ForwardDisseminateMessage) protocolMessage;
        disseminate(message.getTopic(), message.getTopic());
    };

    private void sendMessageIfNotMe(ProtocolMessage protocolMessage, Host host) {
        if (!host.equals(myself))
            sendMessageSideChannel(protocolMessage, host);
    }

    private ProtocolMessageHandler uponDeliverMessage = (protocolMessage) -> {
        DeliverMessage message = (DeliverMessage) protocolMessage;
        triggerNotification(new MessageDeliver(message.getTopic(), message.getTopic()));
    };

}
