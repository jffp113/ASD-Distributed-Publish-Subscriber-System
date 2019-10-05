package client;

import babel.Babel;

import java.util.Properties;
import java.util.Scanner;

public class AutomatedClient {
    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";

    public static final int MESSAGES = 100000;
    public static final int WAIT_TIME = 1000;

    public static void main(String[] args) throws Exception {
        Scanner in = new Scanner(System.in);
        Properties prop = Babel.getInstance().loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        Client c = new Client(args);

        publisherAuto(prop, c);
    }


    public static void publisherAuto(Properties prop, Client c) throws Exception {
        int processID = Integer.parseInt(prop.getProperty("listen_base_port"));

        Thread.sleep(2000);
        c.subscribe("all");
        for (int i = 0; i < MESSAGES; i++) {
            System.out.println("Publish=" + "all=" + i + "=" + processID + "=Time=" + System.currentTimeMillis());
            c.publish("all", i + "=" + processID + "=Time=" + System.currentTimeMillis());
        }

    }

    public static void subscriberAuto(Properties prop, Client c) throws Exception {
        int processID = Integer.parseInt(prop.getProperty("listen_base_port"));
        c.subscribe("all");
    }

}
