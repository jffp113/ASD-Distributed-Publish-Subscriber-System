package protocols.dht;

import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.protocol.GenericProtocol;
import network.Host;
import network.INetwork;
import protocols.dht.messages.FindSuccessorRequestMessage;
import protocols.dht.messages.FindSuccessorResponseMessage;
import protocols.dht.messages.FingerTableRequestMessage;
import protocols.dht.requests.RouteRequest;
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

        //registerMessageHandler(FingerTableResponseMessage.MSG_CODE, uponFingerTableResponseMessage, BCastProtocolMessage.serializer);
        //registerMessageHandler(FingerTableRequestMessage.MSG_CODE, uponFingerTableRequestMessage, BCastProtocolMessage.serializer);
        registerMessageHandler(FindSuccessorRequestMessage.MSG_CODE, uponFindSuccessorRequestMessage, FindSuccessorRequestMessage.serializer);
        registerMessageHandler(FindSuccessorResponseMessage.MSG_CODE, uponFindSuccessorResponseMessage, FindSuccessorResponseMessage.serializer);

    }

    @Override
    public void init(Properties properties) {
        myId = Util.calculateID(myself.toString());
        fingers = new ArrayList<>(Util.fingers);
        try{
            String contact = properties.getProperty("Contact");
            if(contact == null)
                createRing();
            String[] contactSplit = contact.split(":");
            Host host = new Host(InetAddress.getByName(contactSplit[0]), Integer.parseInt(contactSplit[1]));
            sendMessageSideChannel(new FindSuccessorRequestMessage(myId, myself), host);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

//    private void join(Host node) {
//        sendMessageSideChannel(new FingerTableRequestMessage(), node);
//    }

    private void createRing(){
        for (int i = 1; i <= Util.fingers; i++) {
            int begin = Util.calculateFinger(myId, i);
            int end = Util.calculateFinger(myId, i);
            fingers.add(new FingerEntry(begin, begin, end, myId, myself));
        }
        predecessor = myself;
    }

//    private final ProtocolMessageHandler uponFingerTableResponseMessage = (protocolMessage) -> {
//        FingerTableResponseMessage responseMessage = (FingerTableResponseMessage) protocolMessage;
//        initFingerTable(responseMessage.getFingers(),responseMessage.getPredecessor());
//        updateOthers();
//
//    };

//    private final ProtocolMessageHandler uponFingerTableRequestMessage = (protocolMessage) -> {
//        FingerTableRequestMessage requestMessage = (FingerTableRequestMessage) protocolMessage;
//        FingerTableResponseMessage responseMessage = new FingerTableResponseMessage(this.fingers, this.predecessor);
//        sendMessageSideChannel(responseMessage, requestMessage.getFrom());
//    };

    private final ProtocolMessageHandler uponFindSuccessorRequestMessage = (protocolMessage) -> {
        FindSuccessorRequestMessage message = (FindSuccessorRequestMessage) protocolMessage;
        int nodeId = message.getNodeId();
        int successor = fingers.get(0).suc;

        if (isIdBetween(nodeId)) { // pre < nodeid < me
            sendMessageSideChannel(new FindSuccessorResponseMessage(myId,this.fingers,this.predecessor),
                    message.getRequesterNode());
        } else {
            FingerEntry entry = closestPrecedingNode(nodeId);
            sendMessageSideChannel(new FindSuccessorRequestMessage(nodeId, message.getRequesterNode()),
                    entry.h);
        }
    };

    private boolean isIdBetween(int nodeID){
        int predessorID =  Util.calculateIDByHost(predecessor);

        int begin = predessorID < myId ? predessorID : myId;
        int end = predessorID < myId ?  myId : predessorID;

        return begin < nodeID && end > nodeID;
    }

    private final ProtocolMessageHandler uponFindSuccessorResponseMessage = (protocolMessage) -> {
        FindSuccessorResponseMessage message = (FindSuccessorResponseMessage) protocolMessage;
//        join(successor.h);
        initFingerTable(message.getFingers(),message.getPredecessor(),message.getNodeId(),message.getFrom());
        updateOthers();
    };


    private FingerEntry closestPrecedingNode(int nodeId) {
        FingerEntry entry = fingers.get(0);

        for (int i = 0; i < fingers.size(); i++) {
            entry = fingers.get(i);
            if (entry.suc > nodeId) {
                return entry;
            }
        }
        return entry;
    }

    private void updateOthers() {
        for (int i = 1; i <= fingers.size(); i++) {
            FingerEntry p = closestPrecedingNode(Util.calculateFinger(myId, i));


        }
    }

    private void initFingerTable(List<FingerEntry> successorFingerTable, Host predecessor ,int hostId ,Host successorHost) {
        this.predecessor = predecessor;
        FingerEntry successor = fingers.get(0);
        successor.h = successorHost;
        successor.suc = hostId;

        for(int i = 1 ; i < Util.fingers;i++){
            FingerEntry finger = successorFingerTable.get(i);

        }

    }

    private final ProtocolRequestHandler uponRouteRequest = (protocolRequest) -> {
        RouteRequest request = (RouteRequest) protocolRequest;
    };


}
