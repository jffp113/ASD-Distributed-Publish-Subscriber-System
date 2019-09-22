package protocols.publicsubscriber;

import babel.protocol.GenericProtocol;
import network.INetwork;

import java.util.Properties;

//TODO implement this protocol with the correspondent methods
public class PublishSubscriber extends GenericProtocol{

    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Publish/Subscriber";


    public PublishSubscriber(INetwork net) {
        super(PROTOCOL_NAME, PROTOCOL_ID, net);
    }

    @Override
    public void init(Properties properties) {
        //TODO this method must be implemented
    }
}
