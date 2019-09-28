package protocols.publishsubscribe;

import babel.protocol.GenericProtocol;
import network.INetwork;

import java.util.Properties;

public class HyParView extends GenericProtocol {

    public static final short PROTO_ID = 1;
    public static final String ALG_NAME = "HyParView";

    public HyParView(INetwork net) {
        super(ALG_NAME, PROTO_ID, net);
    }

    @Override
    public void init(Properties properties) {

    }


}
