package client;

import babel.Babel;

import java.io.*;
import java.util.*;

public class AutomatedClient {
    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";
    public static final String AUTOMATED_CLIENT_CONFIG_PROPERTIES = "src/automated_client.properties";

    private static final int MESSAGES = 100000;
    private static final int WAIT_TIME = 1000;
    private static final String LISTEN_BASE_PORT = "listen_base_port";
    private static final String NODES = "nodes";
    private static final String TOPICS = "topics";
    private static List<PrintWriter> clients;
    private static int nodes;
    private static int numberTopics;
    private static List<Integer> ports;
    private static List<String> topics;

    public static void main(String[] args) throws Exception {
        Babel.getInstance().loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        Properties properties = Babel.getInstance().loadConfig(AUTOMATED_CLIENT_CONFIG_PROPERTIES, args);
        nodes = Integer.parseInt(properties.getProperty(NODES, "10"));
        clients = new ArrayList<>(nodes);
        ports = new ArrayList<>(nodes);
        numberTopics = Integer.parseInt(properties.getProperty(TOPICS, "30"));
        topics = new ArrayList<>(numberTopics);

        initializeNodes();
        initializeTopics();

        subscriberAuto(properties, clients.get(1));
      //  publisherAuto(properties, c);


    }

    public static void publisherAuto(Properties prop, Client c) throws Exception {
        int processID = Integer.parseInt(prop.getProperty(LISTEN_BASE_PORT));
        int topics = Integer.parseInt(prop.getProperty(LISTEN_BASE_PORT));

        Thread.sleep(2000);
        c.subscribe("all");
        for (int i = 0; i < MESSAGES; i++) {
            System.out.println("Publish=" + "all=" + i + "=" + processID + "=Time=" + System.currentTimeMillis());
            c.publish("all", i + "=" + processID + "=Time=" + System.currentTimeMillis());
        }

    }

    public static void subscriberAuto(Properties prop, PrintWriter c) throws Exception {
        c.println("subscribe all");
    }

    public static void unsubscribeAuto(Client c) {
        //temos de guardar os topicos
        c.unsubscribe(null);
    }

    public static void initializeNodes() throws Exception {
        Random r = new Random(System.currentTimeMillis());
        PrintWriter output = createNode(10001, 10000);
        ports.add(10000);
        clients.add(output);
        for (int i = 1; i < nodes; i++) {
            int port = 10000 + i;
            int randomPort = ports.get(r.nextInt(ports.size()));
            ports.add(port);

            output = createNode(randomPort, port);
            clients.add(output);
        }

    }

    public static void initializeTopics() {
        for (int i = 0; i < numberTopics; i++) {
            topics.add("topic" + i);
        }
    }

    public static PrintWriter createNode(int contact, int port) throws IOException, InterruptedException {
        contact+=1000;
        port+=1000;
        Process process = Runtime.getRuntime()
                .exec("java -jar ./target/ASD-1.0-jar-with-dependencies.jar Contact=127.0.0.1:" + contact + " listen_base_port=" + port + " > ./automatedClientOutput/" + port);
        PrintWriter out = new PrintWriter(new OutputStreamWriter(process.getOutputStream()), true);
        out.println("OLAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa");
        out.flush();
        Thread.sleep(1000);
        System.out.println(process.isAlive());
        return out;
    }

}
