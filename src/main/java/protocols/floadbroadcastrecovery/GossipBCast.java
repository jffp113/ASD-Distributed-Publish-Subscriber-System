package protocols.floadbroadcastrecovery;


import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.exceptions.HandlerRegistrationException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolReplyHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolReply;
import babel.requestreply.ProtocolRequest;
import network.Host;
import network.INetwork;
import protocols.floadbroadcastrecovery.messages.BCastProtocolMessage;
import protocols.floadbroadcastrecovery.notifications.BCastDeliver;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import protocols.partialmembership.GlobalMembership;
import protocols.partialmembership.requests.GetSampleReply;
import protocols.partialmembership.requests.GetSampleRequest;

import java.util.*;

public class GossipBCast extends GenericProtocol {

    public final static short PROTOCOL_ID = 200;
    public final static String PROTOCOL_NAME = "Gossip BCast";

    //Parameters
    private int fanout;

    //Protocol State
    private Set<UUID> delivered;
    private Map<UUID, BCastProtocolMessage> pending;


    public GossipBCast(INetwork net) throws HandlerRegistrationException {
        super("GossipBCast", PROTOCOL_ID, net);

        //Register Events

        //Requests
        registerRequestHandler(BCastRequest.REQUEST_ID, uponBCastRequest);

        //Replies
        registerReplyHandler(GetSampleReply.REPLY_ID, uponGetSampleReply);

        //Notifications Produced
        registerNotification(BCastDeliver.NOTIFICATION_ID, BCastDeliver.NOTIFICATION_NAME);

        //Messages
        registerMessageHandler(BCastProtocolMessage.MSG_CODE, uponBcastProtocolMessage, BCastProtocolMessage.serializer);

        //Timers
        //nothing to be done

    }

    @Override
    public void init(Properties props) {
        //Load parameters
        fanout = Short.parseShort(props.getProperty("fanout", "3"));

        //Initialize State
        this.delivered = new TreeSet<>();
        this.pending = new HashMap<>();

        //setup timers
    }

    private ProtocolRequestHandler uponBCastRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest r) {
            //Create Message
            BCastRequest req = (BCastRequest) r;
            BCastProtocolMessage message = new BCastProtocolMessage();
            message.setPayload(req.getPayload());

            //Deliver message
            delivered.add(message.getMessageId());
            BCastDeliver deliver = new BCastDeliver(req.getPayload());
            triggerNotification(deliver);

            //Make request for peers (to the membership)
            GetSampleRequest request = new GetSampleRequest(fanout, message.getMessageId());
            request.setDestination(GlobalMembership.PROTOCOL_ID);
            pending.put(message.getMessageId(), message);
            try {
                sendRequest(request);
            } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
                destinationProtocolDoesNotExist.printStackTrace();
                System.exit(1);
            }
        }
    };

    private ProtocolReplyHandler uponGetSampleReply = new ProtocolReplyHandler() {
        @Override
        public void uponReply(ProtocolReply reply) {
            GetSampleReply rep = (GetSampleReply) reply;

            //Send message to sample.
            BCastProtocolMessage msg = pending.remove(rep.getRequestID());
            for(Host h: rep.getSample()) {
                sendMessage(msg, h);
            }
        }
    };

    private ProtocolMessageHandler uponBcastProtocolMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage m) {
            BCastProtocolMessage msg = (BCastProtocolMessage) m;

            //check if message was already observed, ignore if yes
            if(!delivered.contains(msg.getMessageId())) {
                //Deliver message
                delivered.add(msg.getMessageId());
                BCastDeliver deliver = new BCastDeliver(msg.getPayload());
                triggerNotification(deliver);

                //Create Request for peers (in the membership)
                GetSampleRequest request = new GetSampleRequest(fanout, msg.getMessageId());
                request.setDestination(GlobalMembership.PROTOCOL_ID);
                pending.put(msg.getMessageId(), msg);
                try {
                    sendRequest(request);
                } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
                    destinationProtocolDoesNotExist.printStackTrace();
                    System.exit(1);
                }
            }
        }
    };

}
