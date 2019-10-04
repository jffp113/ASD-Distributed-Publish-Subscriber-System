package protocols.floadbroadcastrecovery;


import babel.exceptions.DestinationProtocolDoesNotExist;
import babel.exceptions.HandlerRegistrationException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolReplyHandler;
import babel.handlers.ProtocolRequestHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import network.Host;
import network.INetwork;
import persistence.PersistentMap;
import persistence.PersistentSet;
import protocols.floadbroadcastrecovery.messages.BCastProtocolMessage;
import protocols.floadbroadcastrecovery.messages.MessageRequestProtocolMessage;
import protocols.floadbroadcastrecovery.messages.ReBCastProtocolMessage;
import protocols.floadbroadcastrecovery.notifcations.BCastDeliver;
import protocols.floadbroadcastrecovery.requests.BCastRequest;
import protocols.floadbroadcastrecovery.timers.PeriodicPurgeProtocolTimer;
import protocols.floadbroadcastrecovery.timers.PeriodicRebroadcastProtocolTimer;
import protocols.partialmembership.HyParView;
import protocols.partialmembership.requests.GetSampleReply;
import protocols.partialmembership.requests.GetSampleRequest;

import java.util.*;

public class GossipBCast extends GenericProtocol {

    public static final short PROTOCOL_ID = 200;
    public static final String PROTOCOL_NAME = "GossipBCast";
    private static final String FANOUT = "fanout";
    private static final String LISTEN_BASE_PORT = "listen_base_port";
    private static final String DELIVERED_FILE_NAME = "./pers/Delivered";
    public static final String RECOVERY_FILE_NAME = "./pers/Recovery";
    private static final String REBROADCAST_INIT = "rebroadcastInit";
    private static final String REBROADCAST_PERIOD = "rebroadcastPeriod";
    private static final String PURGE_INIT = "purgeInit";
    private static final String PURGE_PERIOD = "purgePeriod";
    private static final int INITIAL_CAPACITY = 100;


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
    private Map<UUID, BCastProtocolMessage> recoveryMSG;


    public GossipBCast(INetwork net) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);
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
        registerTimerHandler(PeriodicRebroadcastProtocolTimer.TIMER_CODE, rebroadcastTimerHandler);
        registerTimerHandler(PeriodicPurgeProtocolTimer.TIMER_CODE, purgeTimerHandler);
        //nothing to be done

    }

    @Override
    public void init(Properties props) {
        //Load parameters
        fanout = Short.parseShort(props.getProperty(FANOUT, "3"));
        //Initialize State
        try {
            int basePort = Integer.parseInt(props.getProperty(LISTEN_BASE_PORT));
            //TODO change these
            this.delivered = new PersistentSet<>(new TreeSet<>(), DELIVERED_FILE_NAME + basePort, 1);
            this.recoveryMSG = new PersistentMap<>(new HashMap<>(INITIAL_CAPACITY), RECOVERY_FILE_NAME + basePort, 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.pending = new HashMap<>();

        //setup timers
        setupPeriodicTimer(new PeriodicRebroadcastProtocolTimer(), Integer.parseInt(props.getProperty(REBROADCAST_INIT, "1000")),
                Integer.parseInt(props.getProperty(REBROADCAST_PERIOD, "2000")));
        setupPeriodicTimer(new PeriodicPurgeProtocolTimer(), Integer.parseInt(props.getProperty(PURGE_INIT, "5000")),
                Integer.parseInt(props.getProperty(PURGE_PERIOD, "100000")));
    }


    private ProtocolTimerHandler rebroadcastTimerHandler = (protocolTimer) -> {
        //Create Request for peers (in the membership)
        ReBCastProtocolMessage request = new ReBCastProtocolMessage(new LinkedList<>(recoveryMSG.keySet()), myself);
        requestMessageBroadcast(request);
    };

    private ProtocolTimerHandler purgeTimerHandler = (protocolTimer) -> {
        this.recoveryMSG.clear();
        this.delivered.clear();
    };


    private ProtocolMessageHandler uponReBcastProtocolMessage = (protocolMessage) -> {
        ReBCastProtocolMessage msg = (ReBCastProtocolMessage) protocolMessage;

        //check if message was already observed, ignore if yes
        if (!delivered.contains(msg.getMessageId())) {
            //Verify If i have all messages from list
            for (UUID id : msg.getMessageUUIDList()) {
                if (!delivered.contains(id)) {
                    addNetworkPeer(msg.getInitialHost());  //TODO conexao temporaria
                    sendMessage(new MessageRequestProtocolMessage(id), msg.getInitialHost());
                }
            }

            //Deliver message
            delivered.add(msg.getMessageId());
            requestMessageBroadcast(msg);
        }
    };

    public ProtocolMessageHandler uponMessageRequest = (protocolMessage) -> sendMessage(recoveryMSG.get(((MessageRequestProtocolMessage) protocolMessage).getMessageId()),
            protocolMessage.getFrom());

    private void requestMessageBroadcast(ReBCastProtocolMessage request) {
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
    private ProtocolRequestHandler uponBCastRequest = (protocolRequest) -> {
        //Create Message
        BCastRequest req = (BCastRequest) protocolRequest;
        BCastProtocolMessage message = new BCastProtocolMessage();
        message.setPayload(req.getPayload());
        //Deliver message
        recoveryMSG.put(message.getMessageId(), message);
        delivered.add(message.getMessageId());
        requestMessageBroadcast(message);
    };

    /**
     * When the protocol receives from the layer bellow the peers that he can talk
     * it will broadcast the message
     */
    private ProtocolReplyHandler uponGetSampleReply = (protocolReply) -> {
        GetSampleReply rep = (GetSampleReply) protocolReply;

        //Send message to sample.
        ProtocolMessage msg = pending.remove(rep.getRequestID());
        for (Host h : rep.getSample()) {
            sendMessage(msg, h);
        }
    };

    /**
     * When the layer below responses with the peers
     * connectected the message will be broadcast to them
     */
    private ProtocolMessageHandler uponBcastProtocolMessage = (protocolMessage) -> {
        BCastProtocolMessage msg = (BCastProtocolMessage) protocolMessage;

        //check if message was already observed, ignore if yes
        if (!delivered.contains(msg.getMessageId())) {
            //Deliver message
            delivered.add(msg.getMessageId());
            requestMessageBroadcast(msg);
        }
    };

    /**
     * This method will request the layer below the peers to broadcast the message
     * and save the message for broadcasting later
     *
     * @param msg
     */
    private void requestMessageBroadcast(BCastProtocolMessage msg) {
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

    private void putMessageOnRecoveryList(BCastProtocolMessage m) {
        this.recoveryMSG.put(m.getMessageId(), m);
    }

    //On the broadcasting a message put it on rebroadcast in order to rebroadcast if a process recovery from a crash

    //Set A timers for rebroadcast de message
    //Rebroadcast only Message ID

    //Create a message in order to response with the message content

    //Which process need to save in a not volatile memory the content of already delivered Messages
    // and rebroadcast messages
}
