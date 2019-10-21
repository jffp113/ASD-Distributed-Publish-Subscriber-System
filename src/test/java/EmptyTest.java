import babel.Babel;
import network.INetwork;
import protocols.dht.ChordWithSalt;
import protocols.floadbroadcastrecovery.GossipBCast;
import protocols.floadbroadcastrecovery.notifcations.BCastDeliver;
import protocols.partialmembership.HyParView;
import protocols.publishsubscribe.PublishSubscribe;
import protocols.publishsubscribe.notifications.PBDeliver;

import java.util.Properties;

public class EmptyTest {
    private static Properties properties;
    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";
    private static final String NOTIFICATION_FORMAT = "Process %s received event at %d: Topic: %s Message: %s\n";
    private static final String LISTEN_BASE_PORT = "listen_base_port";

    public static void main(String[] args) throws Exception {
        Babel babel = Babel.getInstance();
        properties = babel.loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        INetwork net = babel.getNetworkInstance();

        ChordWithSalt hyParView = new ChordWithSalt(net);
        hyParView.init(properties);
        babel.registerProtocol(hyParView);


        babel.start();
    }
}
