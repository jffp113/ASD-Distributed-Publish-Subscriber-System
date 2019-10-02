package protocols.floadbroadcastrecovery;


import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.exceptions.HandlerRegistrationException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolReplyHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import babel.requestreply.ProtocolReply;
import babel.requestreply.ProtocolRequest;
import babel.timer.ProtocolTimer;
import network.Host;
import network.INetwork;
import protocols.floadbroadcastrecovery.messages.BCastProtocolMessage;
import protocols.floadbroadcastrecovery.messages.MessageRequestProtocolMessage;
import protocols.floadbroadcastrecovery.messages.ReBCastProtocolMessage;
import protocols.floadbroadcastrecovery.notifcations.BCastDeliver;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import protocols.floadbroadcastrecovery.timers.PeriodicRebroadcastProtocolTimer;
import protocols.partialmembership.HyParView;
import protocols.partialmembership.requests.GetSampleReply;
import protocols.partialmembership.requests.GetSampleRequest;

import java.util.*;

public class GossipBCast extends GenericProtocol {

    public static final short PROTOCOL_ID = 200;
    public static final String PROTOCOL_NAME = "Gossip BCast";

    //Parameters
    private int fanout;

    //Protocol State
    //TODO
    private Set<UUID> delivered;
    private Map<UUID, ProtocolMessage> pending;

    /**
     * This List will contain messages to reBroadcast.
     * We can use lazy push to send duplicates messages.
      */
    private Map<UUID,BCastProtocolMessage> recoveryMSG;


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
        registerMessageHandler(ReBCastProtocolMessage.MSG_CODE, uponReBcastProtocolMessage, ReBCastProtocolMessage.serializer);
        registerMessageHandler(MessageRequestProtocolMessage.MSG_CODE, uponMessageRequest, MessageRequestProtocolMessage.serializer);

        //Timers
        registerTimerHandler(PeriodicRebroadcastProtocolTimer.TimerCode,timerHandler);
        //nothing to be done

    }

    @Override
    public void init(Properties props) {
        //Load parameters
        fanout = Short.parseShort(props.getProperty("fanout", "3"));
        //Initialize State
        this.delivered = new TreeSet<>();
        this.pending = new HashMap<>();
        this.recoveryMSG = new HashMap<>(100);

        //setup timers
        setupPeriodicTimer(new PeriodicRebroadcastProtocolTimer(),1000,2000);
    }


    private ProtocolTimerHandler timerHandler = new ProtocolTimerHandler(){
        @Override
        public void uponTimer(ProtocolTimer protocolTimer) {
            //Create Request for peers (in the membership)
            ReBCastProtocolMessage request = new ReBCastProtocolMessage(new LinkedList<>(recoveryMSG.keySet()),myself);
            requestMessageBroadcast(request);
        }
    };


    private ProtocolMessageHandler uponReBcastProtocolMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage m) {
            ReBCastProtocolMessage msg = (ReBCastProtocolMessage) m;

            //check if message was already observed, ignore if yes
            if(!delivered.contains(msg.getMessageId())) {
                //Verify If i have all messages from list
                for(UUID id : msg.getMessageUUIDList()){
                    if(!delivered.contains(id)){
                        addNetworkPeer(msg.getInitialHost());  //TODO
                        sendMessage(new MessageRequestProtocolMessage(id),msg.getInitialHost());
                    }  
                }

                //Deliver message
                delivered.add(msg.getMessageId());
                requestMessageBroadcast(msg);
            }
        }
    };

    public ProtocolMessageHandler uponMessageRequest = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage protocolMessage) {
           sendMessage(recoveryMSG.get(((MessageRequestProtocolMessage)protocolMessage).getMessageId()),
                  protocolMessage.getFrom());
        }
    };



    private void requestMessageBroadcast(ReBCastProtocolMessage request){
        pending.put(request.getMessageId(), request);

        GetSampleRequest requestPeers = new GetSampleRequest(fanout, request.getMessageId());
        requestPeers.setDestination(HyParView.PROTOCOL_ID);

        try {
            sendRequest(requestPeers);
        } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
            destinationProtocolDoesNotExist.printStackTrace();
            System.exit(1);
        }
    }


    /**
     * This Handler will receive Requests from the layer above
     * It will gossip the message to some network members using
     * a network overlay constructed on the layer bellow
     * When Gossiping it will save the message for rebroadcasting
     */
    private ProtocolRequestHandler uponBCastRequest = new ProtocolRequestHandler() {
        @Override
        public void uponRequest(ProtocolRequest r) {
            //Create Message
            BCastRequest req = (BCastRequest) r;
            BCastProtocolMessage message = new BCastProtocolMessage();
            message.setPayload(req.getPayload());
            //Deliver message
            recoveryMSG.put(message.getMessageId(),message);
            delivered.add(message.getMessageId());
            requestMessageBroadcast(message);
        }
    };

    /**
     * When the protocol receives from the layer bellow the peers that he can talk
     * it will broadcast the message
     */
    private ProtocolReplyHandler uponGetSampleReply = new ProtocolReplyHandler() {
        @Override
        public void uponReply(ProtocolReply reply) {
            GetSampleReply rep = (GetSampleReply) reply;

            //Send message to sample.
            ProtocolMessage msg = pending.remove(rep.getRequestID());
            for(Host h: rep.getSample()) {
                sendMessage(msg, h);
            }
        }
    };

    /**
     * When the layer below responses with the peers
     * connectected the message will be broadcast to them
     */
    private ProtocolMessageHandler uponBcastProtocolMessage = new ProtocolMessageHandler() {
        @Override
        public void receive(ProtocolMessage m) {
            BCastProtocolMessage msg = (BCastProtocolMessage) m;

            //check if message was already observed, ignore if yes
            if(!delivered.contains(msg.getMessageId())) {
                //Deliver message
                delivered.add(msg.getMessageId());
                requestMessageBroadcast(msg);
            }
        }
    };

    /**
     * This method will request the layer below the peers to broadcast the message
     * and save the message for broadcasting later
     * @param msg
     */
    private void requestMessageBroadcast(BCastProtocolMessage msg){
        BCastDeliver deliver = new BCastDeliver(msg.getPayload());
        triggerNotification(deliver);

        //Create Request for peers (in the membership)
        GetSampleRequest request = new GetSampleRequest(fanout, msg.getMessageId());
        request.setDestination(HyParView.PROTOCOL_ID);
        pending.put(msg.getMessageId(), msg);
        try {
            sendRequest(request);
        } catch (DestinationProtocolDoesNotExist destinationProtocolDoesNotExist) {
            destinationProtocolDoesNotExist.printStackTrace();
            System.exit(1);
        }
    }

    private void putMessageOnRecoveryList(BCastProtocolMessage m){
        this.recoveryMSG.put(m.getMessageId(),m);
    }

    //On the broadcasting a message put it on rebroadcast in order to rebroadcast if a process recovery from a crash

    //Set A timers for rebroadcast de message
    //Rebroadcast only Message ID

    //Create a message in order to response with the message content

    //Which process need to save in a not volatile memory the content of already delivered Messages
    // and rebroadcast messages
}
