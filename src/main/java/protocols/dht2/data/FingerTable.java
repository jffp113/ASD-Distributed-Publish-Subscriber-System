package protocols.dht2.data;
import babel.Babel;
import babel.handlers.ProtocolMessageHandler;
import babel.protocol.event.ProtocolMessage;
import network.Host;
import network.INetwork;
import network.INodeListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht2.data.Handler.MessageHandler;
import protocols.dht2.data.message.FindSuccessorRequestMessage;
import protocols.dht2.data.message.FindSuccessorResponseMessage;
import utils.PropertiesUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static utils.PropertiesUtils.CONTACT;

public class FingerTable implements INodeListener {

    final static Logger logger = LogManager.getLogger(FingerTable.class.getName());
    private static final int MAX_BITS_OF_ID = PropertiesUtils.getPropertyAsInt(PropertiesUtils.MAX_BITS_OF_ID_PROPERTY);


    private FingerEntry[] fingers;

    private Host predecessorHost;
    private ID predecessorID;

    /**
        Counts references to TCP connection in order to Garbage collect it
     */
    private Map<String, AtomicInteger> tcpConnectionReferences;
    private INetwork iNetwork;
    private ID localID;


    public FingerTable(ID localID) throws Exception {
        this.fingers = new FingerEntry[MAX_BITS_OF_ID];
        this.localID = localID;
        predecessorHost = null;
        predecessorID = null;
        iNetwork = Babel.getInstance().getNetworkInstance();
        iNetwork.registerNodeListener(this);
        tcpConnectionReferences = new HashMap<>();
    }

    public void init(MessageHandler handle) throws UnknownHostException {
        handle.handle(FindSuccessorRequestMessage.MSG_CODE,
                                 uponFindSuccessorRequestMessage, FindSuccessorRequestMessage.serializer);
        handle.handle(FindSuccessorResponseMessage.MSG_CODE,
                  uponFindSuccessorResponseMessage, FindSuccessorResponseMessage.serializer);

        create();

        String contactString = PropertiesUtils.getPropertyAsString(CONTACT);

        if (contactString != null) {
            String[] contactSplit = contactString.split(":");
            Host contact = new Host(InetAddress.getByName(contactSplit[0]), Integer.parseInt(contactSplit[1]));
            join(contact);
        }

    }
    private void create() {
        predecessorID = null;
        predecessorHost = null;
        for(int i = 0 ; i < fingers.length;i++){
            this.fingers[i] = FingerEntry.genFingerEntryToHost(localID,iNetwork.myHost(),i);
        }
        updateSucessor(iNetwork.myHost());
    }

    private void join(Host contact) {
        sendMessageSideChannel(new FindSuccessorRequestMessage(localID,iNetwork.myHost()),contact);
    }


    private ProtocolMessageHandler uponFindSuccessorRequestMessage = protocolMessage -> {
        FindSuccessorRequestMessage message = (FindSuccessorRequestMessage) protocolMessage;
        ID nodeId = message.getNodeId();
        ID successorId = getSuccessor().getHostId();

        if (nodeId.isInInterval(localID,successorId)) {
            sendMessageSideChannel(new FindSuccessorResponseMessage(getSuccessor().getHost(), Arrays.asList(fingers)),message.getRequesterNode());
        }else{
            FingerEntry closestPrecedingNode = getClosestPrecedingNode(nodeId);
            if (closestPrecedingNode != null && !iNetwork.myHost().equals(closestPrecedingNode.getHost())) {
                sendMessage(message, closestPrecedingNode.getHost());
            }
        }
    };

    private ProtocolMessageHandler uponFindSuccessorResponseMessage = protocolMessage -> {
        FindSuccessorResponseMessage message = (FindSuccessorResponseMessage) protocolMessage;
        updateSucessor(message.getSuccessor());
        fillMyTable(message.getFingerEntryList());

    };

    private void fillMyTable(List<FingerEntry> fingerEntryList) {

        for(int i = 1 ; i < ID.maxIDSize() - 1;i++){
            FingerEntry f = this.getFingerEntryAt(i);
            FingerEntry listSuccessor = findSuccessorInList(f.getStartId(), fingerEntryList);
            this.updateFingerEntryAt(i,listSuccessor.getHost());
        }

    }

    private FingerEntry findSuccessorInList(ID start, List<FingerEntry> fingerEntryList) {
        for (int i = 0; i < ID.maxIDSize(); i++) {
            FingerEntry f = this.getFingerEntryAt(i);

            if (start.compareTo(f.getStartId())<= 0) {
                return f;
            }
        }
        return fingerEntryList.get(0);
    }


    public final FingerEntry getClosestPrecedingNode(ID key) {
        if (key == null) {
            this.logger.error("Null pointer Id");
        }

        for (int i = this.fingers.length - 1; i >= 0; i--) {
            if (this.fingers[i].getHostId() != null
                    && this.fingers[i].getHostId().isInInterval(
                    this.localID, key)) {
                    this.logger.info("Closest preceding node for ID " + key + " is " + this.fingers[i].toString());
                return this.fingers[i];
            }
        }

        this.logger.info("No closing node for " + key);

        return null;
    }

    public FingerEntry getSuccessor(){
        return fingers[0];
    }

    public Host getPredecessorHost(){
        return this.predecessorHost;
    }

    public ID getPredecessorID(){
        return this.predecessorID;
    }

    public void changePredecessor(Host host){
        removeConnection(getPredecessorHost());
        this.predecessorID = new ID(host);
        this.predecessorHost = host;
        addConnection(host);
    }


    public FingerEntry getFingerEntryAt(int i){
        return fingers[i];
    }

    public void updateFingerEntryAt(int i, Host host){
        removeConnection(fingers[i].getHost());
        fingers[i].setHost(host);
        addConnection(host);
    }

    public void updateSucessor(Host host){
        updateFingerEntryAt(0,host);
    }

    private void removeConnection(Host host){
        if(host == null)
            return;

        AtomicInteger refCounter = tcpConnectionReferences.get(host.toString());

        if(refCounter == null){
            logger.error("No host connected: " + host);
            return;
        }
        int ref = refCounter.decrementAndGet();

        if(ref == 0){
            logger.info("Removing TCP connection from " + host);
            iNetwork.removePeer(host);
            tcpConnectionReferences.remove(host.toString());
        }
    }

    private void addConnection(Host host){
        if(host == null)
            return;

        AtomicInteger refCounter = tcpConnectionReferences.get(host.toString());

        if(refCounter == null){
            logger.info(String.format("[%s] Adding TCP connection from %s",iNetwork.myHost(),host));
            iNetwork.addPeer(host);
            refCounter = new AtomicInteger(0);
        }
        refCounter.incrementAndGet();

        iNetwork.addPeer(host);
    }

    @Override
    public void nodeDown(Host host) {//TOSEE
        if(predecessorHost.equals(host)){
            predecessorHost = null;
            predecessorID = null;
        }

        tcpConnectionReferences.remove(host.toString());
        removeConnection(host);

        /**
         *
             if (successor.equals(host)) {
             changeSuccessor(myself);
             }

             for (FingerEntry finger : fingers) {
             Host fingerHost = finger.getHost();
             if (fingerHost.equals(host)) {
             Host newHost = getNewHostFromTable(host);
             updateFingerNetworkPeer(fingerHost, newHost);
             finger.setHost(newHost);
             finger.setHostId(calculateId(newHost.toString()));
             }
             }
         */
    }

    @Override
    public void nodeUp(Host host) {

    }

    @Override
    public void nodeConnectionReestablished(Host host) {

    }

    protected final void sendMessage(ProtocolMessage msg, Host destination) {
        this.iNetwork.sendMessage(msg.getId(), msg, destination);
    }

    protected final void sendMessageSideChannel(ProtocolMessage msg, Host destination) {
        this.iNetwork.sendMessage(msg.getId(), msg, destination, true);
    }
}
