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
import protocols.floadbroadcastrecovery.timers.PeriodicRebroadcastProtocolTimer;
import protocols.partialmembership.HyParView;
import protocols.partialmembership.requests.GetSampleReply;
import protocols.partialmembership.requests.GetSampleRequest;
import utils.PropertiesUtils;

import java.util.*;

public class GossipBCast extends GenericProtocol {

    public static final short PROTOCOL_ID = 200;
    public static final String PROTOCOL_NAME = "GossipBCast";

    private static final String FANOUT = "fanout";
    private static final String LISTEN_BASE_PORT = "listen_base_port";
    private static final String DELIVERED_FILE_NAME = "./pers/Delivered";
    private static final String RECOVERY_FILE_NAME = "./pers/Recovery";
    private static final String REBROADCAST_INIT = "rebroadcastInit";
    private static final String REBROADCAST_PERIOD = "rebroadcastPeriod";
    private static final int INITIAL_CAPACITY = 100;

    //Parameters
    private int fanout;

    //Protocol State
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
        //nothing to be done

    }

    @Override
    public void init(Properties props) {
        //Load parameters
        fanout = PropertiesUtils.getPropertyAsInt(props, FANOUT);
        //Initialize State
        try {
            int basePort = PropertiesUtils.getPropertyAsInt(props, LISTEN_BASE_PORT);
            this.delivered = new PersistentSet<>(new TreeSet<>(), DELIVERED_FILE_NAME + basePort);
            this.recoveryMSG = new PersistentMap<>(new HashMap<>(INITIAL_CAPACITY), RECOVERY_FILE_NAME + basePort);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.pending = new HashMap<>();

        //setup timers
        setupPeriodicTimer(new PeriodicRebroadcastProtocolTimer(), PropertiesUtils.getPropertyAsInt(props, REBROADCAST_INIT),
                PropertiesUtils.getPropertyAsInt(props, REBROADCAST_PERIOD));
    }


    private ProtocolTimerHandler rebroadcastTimerHandler = (protocolTimer) -> {
        //Create Request for peers (in the membership)
        ReBCastProtocolMessage request = new ReBCastProtocolMessage(new LinkedList<>(recoveryMSG.keySet()), myself);
        requestReBMessageBroadcast(request);
    };

    private ProtocolMessageHandler uponReBcastProtocolMessage = (protocolMessage) -> {
        ReBCastProtocolMessage msg = (ReBCastProtocolMessage) protocolMessage;

        //check if message was already observed, ignore if yes
        if (!delivered.contains(msg.getMessageId())) {
            //Verify If i have all messages from list
            for (UUID id : msg.getMessageUUIDList()) {
                if (!delivered.contains(id)) {
                    sendMessageSideChannel(new MessageRequestProtocolMessage(id), msg.getInitialHost());
                }
            }

            //Deliver message
            delivered.add(msg.getMessageId());
            requestReBMessageBroadcast(msg);
        }
    };

    public ProtocolMessageHandler uponMessageRequest = (protocolMessage) -> {
        MessageRequestProtocolMessage messageRequestProtocolMessage = (MessageRequestProtocolMessage) protocolMessage;
        BCastProtocolMessage message = recoveryMSG.get(messageRequestProtocolMessage.getMessageId());

        sendMessageSideChannel(message, messageRequestProtocolMessage.getFrom());
    };

    private void requestReBMessageBroadcast(ReBCastProtocolMessage msg) {
        pending.put(msg.getMessageId(), msg);
        sendRequestToPeers(msg.getMessageId());
    }

    /**
     * This method will requests the layer below the peers to broadcast the message
     * and save the message for broadcasting later
     *
     * @param msg
     */
    private void requestMessageBroadcast(BCastProtocolMessage msg) {
        BCastDeliver deliver = new BCastDeliver(msg.getPayload());
        triggerNotification(deliver);

        pending.put(msg.getMessageId(), msg);
        sendRequestToPeers(msg.getMessageId());
    }

    private void sendRequestToPeers(UUID msgID) {
        GetSampleRequest request = new GetSampleRequest(fanout, msgID);
        request.setDestination(HyParView.PROTOCOL_ID);

        try {
            sendRequest(request);
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
        putMessageOnRecoveryList(message);
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
     * connected the message will be broadcast to them
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


    private void putMessageOnRecoveryList(BCastProtocolMessage msg) {
        this.recoveryMSG.put(msg.getMessageId(), msg);
    }

    //On the broadcasting a message put it on rebroadcast in order to rebroadcast if a process recovery from a crash

    //Set A timers for rebroadcast de message
    //Rebroadcast only Message ID

    //Create a message in order to response with the message content

    //Which process need to save in a not volatile memory the content of already delivered Messages
    // and rebroadcast messages
}
