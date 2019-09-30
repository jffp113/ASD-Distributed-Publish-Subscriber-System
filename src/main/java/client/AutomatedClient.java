package client;

import babel.Babel;

import java.util.HashSet;
import java.util.Properties;

public class AutomatedClient {


    public static final int MESSAGES = 100000;
    public static final int WAIT_TIME = 1000;

    public static void main(String[] args) throws Exception{
        Properties prop = Babel.getInstance().loadConfig("src/network_config.properties",args);
        Client c = new Client(args);


            publisherAuto(prop,c);
    }


    public static void publisherAuto(Properties prop, Client c) throws Exception{
        int processID = Integer.parseInt(prop.getProperty("listen_base_port"));

        Thread.sleep(2000);
        c.subscribe("all");
        for(int i = 0; i < MESSAGES; i++){
            System.out.println("Publish=" + "all=" + i + "=" + processID + "=Time="+ System.currentTimeMillis());
            c.publish("all" ,i + "=" + processID+"=Time="+ System.currentTimeMillis());
        }

    }

    public static void subscriberAuto(Properties prop, Client c) throws Exception{
        int processID = Integer.parseInt(prop.getProperty("listen_base_port"));
        c.subscribe("all");
    }

}
