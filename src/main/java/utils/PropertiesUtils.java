package utils;

import babel.Babel;
import babel.exceptions.InvalidParameterException;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtils {
    public static final String MAX_BITS_OF_ID_PROPERTY = "m";
    public static final String CONTACT = "Contact";
    public static final String STABILIZE_INIT = "stabilizeInit";
    public static final String STABILIZE_PERIOD = "stabilizePeriod";
    public static final String FIX_FINGERS_INIT = "fixFingersInit";
    public static final String FIX_FINGERS_PERIOD = "fixFingersPeriod";

    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";

    static Babel babel;
    private static Properties properties;
    static {
        babel = Babel.getInstance();
    }

    public static void loadProperties(String[] args) throws IOException, InvalidParameterException {
        properties = babel.loadConfig(NETWORK_CONFIG_PROPERTIES, args);
    }

    public static String getPropertyAsString(String key) {
        return properties.getProperty(key);
    }


    public static String getPropertyAsString(Properties properties, String key) {
        return properties.getProperty(key);
    }

    public static int getPropertyAsInt(String key) {
        return Integer.parseInt(getPropertyAsString(properties, key));
    }


    public static int getPropertyAsInt(Properties properties, String key) {
        return Integer.parseInt(getPropertyAsString(properties, key));
    }

    public static boolean getPropertyAsBool(String key) {
        return Boolean.valueOf(getPropertyAsString(properties, key));
    }

    public static boolean getPropertyAsBool(Properties properties, String key) {
        return Boolean.valueOf(getPropertyAsString(properties, key));
    }



}
