package protocols.dht;

import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.protocol.GenericProtocol;
import network.Host;
import network.INetwork;
import protocols.dht.messages.FingerTableResponseMessage;
import protocols.dht.requests.RouteRequest;
import protocols.floadbroadcastrecovery.messages.BCastProtocolMessage;
import protocols.floadbroadcastrecovery.requests.BCastRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ChordWithSalt extends GenericProtocol{

    private List<FingerEntry> fingers;
    //TODO Suc list for fail
    private Host predecessor;
    private int myId;

    public ChordWithSalt(String protoName, short protoID, INetwork net) throws Exception{
        super(protoName, protoID, net);
        registerRequestHandler(BCastRequest.REQUEST_ID, uponRouteRequest);
        registerMessageHandler(FingerTableResponseMessage.MSG_CODE, uponFingerTableResponseMessage, BCastProtocolMessage.serializer);
    }

    @Override
    public void init(Properties properties) {
        fingers = new ArrayList<>(Util.fingers);
        join(properties.getProperty("Contact"));
    }

    private void join(String contact) {
        if(contact != null){

        }else {
            for(int i = 1; i <= Util.fingers; i++){
                int begin = Util.calculateFinger(myId,i);
                int end = Util.calculateFinger(myId,i);
                fingers.add(new FingerEntry(begin,begin,end,myId,myself));
            }
            predecessor = myself;
        }
    }

    private final ProtocolMessageHandler uponFingerTableResponseMessage = (protocolMessage) -> {
        initFingers(protocolMessage.getFrom());
        updateOthers();
    };

    private void updateOthers() {

    }

    private void initFingers(Host contact) {

    }


    private final ProtocolRequestHandler uponRouteRequest = (protocolRequest) -> {
       RouteRequest request = (RouteRequest) protocolRequest;
    };



}
