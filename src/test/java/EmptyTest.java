import babel.Babel;
import network.INetwork;
import org.junit.Test;
import persistence.PersistentMap;
import protocols.multipaxos.MultiPaxos;

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class EmptyTest {

    @Test
    public void test1() throws Exception{
        PersistentMap<String,String> map = new PersistentMap<>("1");

        int a = map.put("Jorge", new String("1"));
        map.put("Jorge", new String("2"));
        map.put("Jorge", new String("3"));
        map.put("Jorge", new String("4"));
        map.put("Jorge", new String("5"));
        map.put("Jorge", new String("6"));
        map.put("Jorge", new String("7"));
        map.put("Jorge", new String("8"));
        map.put("Jorge", new String("9"));
        map.put("Jorge", new String("10"));
        map.put("Jorge", new String("11"));
        map.put("Jorge", new String("12"));
        map.put("Jorge", new String("13"));
        Thread.sleep(1);
        List<String> list = map.get("Jorge",5,10);
        System.out.println(list);
        list = map.get("Jorge",1,10);
        Map<String,byte[]> result =  map.getState();
        System.out.println(list);
    }

    public static void main(String[] args) throws Exception {
        Babel babel = Babel.getInstance();
        Properties properties = babel.loadConfig("src/network_config.properties", args);
        INetwork net = babel.getNetworkInstance();

        MultiPaxos paxos = new MultiPaxos(net);
        paxos.init(properties);
        babel.registerProtocol(paxos);

        babel.start();
    }
}
