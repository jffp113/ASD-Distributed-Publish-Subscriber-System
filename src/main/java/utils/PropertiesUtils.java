package utils;

import java.util.Properties;

public class PropertiesUtils {

    public static String getPropertyAsString(Properties properties, String key) {
        return properties.getProperty(key);
    }

    public static int getPropertyAsInt(Properties properties, String key) {
        return Integer.parseInt(getPropertyAsString(properties, key));
    }
}
