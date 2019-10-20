package protocols.dht;

import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.protocol.GenericProtocol;
import network.Host;
import network.INetwork;
import protocols.dht.messages.FindSuccessorRequestMessage;
import protocols.dht.messages.FindSuccessorResponseMessage;
import protocols.dht.messages.FingerTableRequestMessage;
import protocols.dht.messages.FingerTableResponseMessage;
import protocols.dht.requests.RouteRequest;
import protocols.floadbroadcastrecovery.messages.BCastProtocolMessage;
import protocols.floadbroadcastrecovery.requests.BCastRequest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ChordWithSalt extends GenericProtocol {

    private List<FingerEntry> fingers;
    //TODO Suc list for fail
    private Host predecessor;
    private int myId;

    public ChordWithSalt(String protoName, short protoID, INetwork net) throws Exception {
        super(protoName, protoID, net);
        registerRequestHandler(BCastRequest.REQUEST_ID, uponRouteRequest);
        registerMessageHandler(FingerTableResponseMessage.MSG_CODE, uponFingerTableResponseMessage, BCastProtocolMessage.serializer);
        registerMessageHandler(FingerTableRequestMessage.MSG_CODE, uponFingerTableRequestMessage, BCastProtocolMessage.serializer);
        registerMessageHandler(FindSuccessorRequestMessage.MSG_CODE, uponFindSuccessorRequestMessage, FindSuccessorRequestMessage.serializer);
        registerMessageHandler(FindSuccessorResponseMessage.MSG_CODE, uponFindSuccessorResponseMessage, FindSuccessorResponseMessage.serializer);

    }

    @Override
    public void init(Properties properties) {
        fingers = new ArrayList<>(Util.fingers);
        try {

            String[] contactSplit = properties.getProperty("Contact").split(":");
            Host host = new Host(InetAddress.getByName(contactSplit[0]), Integer.parseInt(contactSplit[1]));

            sendMessageSideChannel(new FindSuccessorRequestMessage(myId, myself), host);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private void join(Host node) {
        if (node != null) {
            sendMessageSideChannel(new FingerTableRequestMessage(), node);
        } else {
            for (int i = 1; i <= Util.fingers; i++) {
                int begin = Util.calculateFinger(myId, i);
                int end = Util.calculateFinger(myId, i);
                fingers.add(new FingerEntry(begin, begin, end, myId, myself));
            }
            predecessor = myself;
        }
    }

    private final ProtocolMessageHandler uponFingerTableResponseMessage = (protocolMessage) -> {
        FingerTableResponseMessage responseMessage = (FingerTableResponseMessage) protocolMessage;
        initFingers(responseMessage.getFingers());
        updateOthers();
    };

    private final ProtocolMessageHandler uponFingerTableRequestMessage = (protocolMessage) -> {
        FingerTableRequestMessage requestMessage = (FingerTableRequestMessage) protocolMessage;
        FingerTableResponseMessage responseMessage = new FingerTableResponseMessage(this.fingers, this.predecessor);
        sendMessageSideChannel(responseMessage, requestMessage.getFrom());
    };

    private final ProtocolMessageHandler uponFindSuccessorRequestMessage = (protocolMessage) -> {
        FindSuccessorRequestMessage message = (FindSuccessorRequestMessage) protocolMessage;
        int nodeId = message.getNodeId();
        int successor = fingers.get(0).suc;
        if (nodeId >= this.myId && nodeId < successor) {
            sendMessageSideChannel(new FindSuccessorResponseMessage(myId), message.getRequesterNode());
        } else {
            FingerEntry entry = closestPrecedingNode(nodeId);
            sendMessageSideChannel(new FindSuccessorRequestMessage(nodeId, message.getRequesterNode()),
                    entry.h);
        }
    };

    private final ProtocolMessageHandler uponFindSuccessorResponseMessage = (protocolMessage) -> {

    };

    private FingerEntry closestPrecedingNode(int nodeId) {
        for (int i = fingers.size() - 1; i > 0; i--) {
            FingerEntry entry = fingers.get(i);
            if (entry.suc >= myId && entry.suc <= nodeId) {
                return entry;
            }
        }
        return null;
    }


    private void updateOthers() {

    }

    private void initFingers(List<FingerEntry> successorFingerTable) {

    }

    private final ProtocolRequestHandler uponRouteRequest = (protocolRequest) -> {
        RouteRequest request = (RouteRequest) protocolRequest;
    };


}
