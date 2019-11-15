package protocols.dht2;

import babel.exceptions.HandlerRegistrationException;
import babel.handlers.ProtocolMessageHandler;
import babel.handlers.ProtocolTimerHandler;
import babel.protocol.GenericProtocol;
import babel.protocol.event.ProtocolMessage;
import network.Host;
import network.INetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht2.data.FingerEntry;
import protocols.dht2.message.*;
import protocols.dht2.timers.StabilizeTimer;
import protocols.dht2.data.FingerTable;
import protocols.dht2.data.ID;
import protocols.dht2.timers.FixFingersTimer;
import protocols.partialmembership.timers.DebugTimer;
import utils.PropertiesUtils;
import java.util.Properties;
import static utils.PropertiesUtils.*;

public class Chord extends GenericProtocol {
    final static Logger logger = LogManager.getLogger(protocols.dht2.Chord.class.getName());

    public static final short PROTOCOL_ID = 1243;
    public static final String PROTOCOL_NAME = "Chord";

    private ID myID;
    private FingerTable fingerTable;

    public Chord(INetwork net) throws Exception {
        super(PROTOCOL_NAME,PROTOCOL_ID, net);
        myID = new ID(myself);
        fingerTable = new FingerTable(myID);
        fingerTable.init((id,handler,serializer)-> {
            try {
                this.registerMessageHandler(id,handler,serializer);
            } catch (HandlerRegistrationException e) {
                logger.error(e);
            }
        });
    }

    @Override
    public void init(Properties properties) {
        try {
            System.out.println("Here");
            // Timers
            registerTimerHandler(DebugTimer.TimerCode, uponDebugTimer);

            registerTimerHandler(StabilizeTimer.TimerCode, uponStabilizeTimer);
            registerTimerHandler(FixFingersTimer.TimerCode, uponFixFingersTimer);

            registerMessageHandler(FindFingerSuccessorResponseMessage.MSG_CODE,
                    uponFindFingerSuccessorResponseMessage,FindFingerSuccessorResponseMessage.serializer);
            registerMessageHandler(FindFingerSuccessorRequestMessage.MSG_CODE,
                    uponFindFingerSuccessorRequestMessage, FindFingerSuccessorRequestMessage.serializer);
            registerMessageHandler(FindPredecessorResponseMessage.MSG_CODE,
                    uponFindPredecessorResponseMessage, FindPredecessorResponseMessage.serializer);
            registerMessageHandler(NotifyPredecessorMessage.MSG_CODE, uponNotifyPredecessorMessage,
                    NotifyPredecessorMessage.serializer);

            registerMessageHandler(FindPredecessorRequestMessage.MSG_CODE,
                    uponFindPredecessorRequestMessage, FindPredecessorRequestMessage.serializer);
            setupTimers();
        } catch (HandlerRegistrationException e) {
            e.printStackTrace();
        }
    }

    private final ProtocolTimerHandler uponDebugTimer = (protocolTimer) -> {

        logger.info(fingerTable.toString());
    };

    private void setupTimers() {

        setupPeriodicTimer(new StabilizeTimer(), PropertiesUtils.getPropertyAsInt(STABILIZE_INIT),
                PropertiesUtils.getPropertyAsInt(STABILIZE_PERIOD));
        setupPeriodicTimer(new FixFingersTimer(),
                PropertiesUtils.getPropertyAsInt(FIX_FINGERS_INIT),
                PropertiesUtils.getPropertyAsInt(FIX_FINGERS_PERIOD));


        setupPeriodicTimer(new DebugTimer(), 1000, 10000);
    }

    ProtocolMessageHandler uponFindPredecessorRequestMessage = protocolMessage -> {
        Host predecessorToSend = fingerTable.getPredecessorHost() == null ? myself : fingerTable.getPredecessorHost();
        sendMessageSideChannel(new FindPredecessorResponseMessage(predecessorToSend), protocolMessage.getFrom());
    };

    //GOOD
    private final ProtocolMessageHandler uponFindFingerSuccessorRequestMessage = (protocolMessage) -> {
        FindFingerSuccessorRequestMessage message = (FindFingerSuccessorRequestMessage) protocolMessage;
        ID nodeId = message.getNodeId();
        ID successorId = fingerTable.getSuccessor().getHostId();

        if (nodeId.isInInterval(myID,successorId)) {
            FingerEntry entry = fingerTable.getClosestPrecedingNode(nodeId);
            if(entry != null)
                sendMessageSideChannel(new FindFingerSuccessorResponseMessage(entry.getHost(),
                                message.getNext()), message.getRequesterNode());
        } else {
            FingerEntry entry = fingerTable.getClosestPrecedingNode(nodeId);
            if (entry != null && !entry.equals(myself)) {
                sendMessage(message, entry.getHost());
            }
        }
    };

    private final ProtocolMessageHandler uponFindFingerSuccessorResponseMessage = (protocolMessage) -> {
        FindFingerSuccessorResponseMessage message = (FindFingerSuccessorResponseMessage) protocolMessage;
        fingerTable.updateFingerEntryAt(message.getNext(),message.getSuccessor());
    };


    private int next;
    private ProtocolTimerHandler uponFixFingersTimer = protocolTimer -> {
        //Update Next finger entry to be fixed
        next++;
        if(next < ID.maxIDSize()){
            next = 1;
        }
        ID nextID = fingerTable.getFingerEntryAt(next).getHostId();
        FingerEntry finger = fingerTable.getClosestPrecedingNode(nextID);

        if(finger != null)
            sendMessage(new FindFingerSuccessorRequestMessage(nextID,myself,next),
                    fingerTable.getClosestPrecedingNode(nextID).getHost());

    };

    private ProtocolTimerHandler uponStabilizeTimer = protocolTimer -> {
        Host successor = fingerTable.getSuccessor().getHost();
        if(successor != null)
            sendMessage(new FindPredecessorRequestMessage(), fingerTable.getSuccessor().getHost());
    };

    private ProtocolMessageHandler uponFindPredecessorResponseMessage = protocolMessage -> {
        FindPredecessorResponseMessage message = (FindPredecessorResponseMessage) protocolMessage;
        Host predecessor = message.getPredecessor();
        ID predecessorId = new ID(predecessor);
        ID successorID = fingerTable.getSuccessor().getHostId();

        if(predecessorId.isInInterval(myID,successorID)){ //TOSEE
            fingerTable.updateSucessor(predecessor);
        }

        sendMessageIfNotMe(new NotifyPredecessorMessage(),fingerTable.getSuccessor().getHost());
    };

    private ProtocolMessageHandler uponNotifyPredecessorMessage = (protocolMessage) ->{
        ID id = new ID(protocolMessage.getFrom());

        if(fingerTable.getPredecessorHost() == null || id.isInInterval(fingerTable.getPredecessorID(),myID)){
            fingerTable.changePredecessor(protocolMessage.getFrom());
        }
    };

    private void sendMessageIfNotMe(ProtocolMessage message,Host host){
        if (!host.equals(myself)) {
            sendMessage(message, host);
        }
    }

}
