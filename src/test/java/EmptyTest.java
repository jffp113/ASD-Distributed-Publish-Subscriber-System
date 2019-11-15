import babel.Babel;
import babel.exceptions.InvalidParameterException;
import network.INetwork;
import org.junit.Assert;
import org.junit.Test;
import protocols.dht2.Chord;
import utils.PropertiesUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;


public class EmptyTest {
    static int k = 32;
    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";

    private boolean isIdBetween(int id, int start, int end, boolean includeEnd) {
        int minLimit = start;
        int maxLimit = end;

        if (minLimit > maxLimit) {
            int amountToMaxLimit = Math.abs(k - id);
            if (amountToMaxLimit < id) {
                maxLimit = k;
            } else {
                minLimit = -1;
            }
        }

        return includeEnd ?start  ==  end|| id > minLimit && id <= maxLimit : id > minLimit && id < maxLimit;
    }

    @Test
    public void test1(){
       /* HashMap<String, Set<String>> jorge= new HashMap<>(100);
        jorge.put("jorege","jorege");
        jorge.put("claudio","laudio");

        System.out.println(jor);*/

        Assert.assertTrue(isIdBetween(4,0,4,true));
        Assert.assertFalse(isIdBetween(4,0,4,false));
        Assert.assertFalse(isIdBetween(4,0,3,true));
        Assert.assertFalse(isIdBetween(4,0,3,false));
        Assert.assertFalse(isIdBetween(4,7,3,false));
        Assert.assertFalse(isIdBetween(4,4,4,false));
        Assert.assertTrue(isIdBetween(4,4,4,true));
    }

    public static void main(String[] args) throws Exception {
        Babel babel = Babel.getInstance();
        Properties properties = babel.loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        PropertiesUtils.loadProperties(args);
        INetwork net = babel.getNetworkInstance();

        Chord chord = new Chord(net);
        chord.init(properties);
        babel.registerProtocol(chord);
        babel.start();
    }
}
