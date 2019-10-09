package client;

import babel.Babel;
import utils.PropertiesUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class AutomatedClient {
    // Constants
    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";
    private static final String AUTOMATED_CLIENT_CONFIG_PROPERTIES = "src/automated_client.properties";
    private static final String NODES = "nodes";
    private static final String TOPICS = "topics";
    private static final String NODES_TO_PUBLISH="nodes_to_publish";
    private static final String KILL_INIT = "kill_init";
    private static final String KILL_PERIOD = "kill_period";
    private static final String PUBLISH_CMD = "publish %s %s time=%d\n";
    private static final String SUBSCRIBE_CMD = "subscribe %s\n";
    private static final String UNSUBSCRIBE_CMD = "unsubscribe %s";
    private static final String MESSAGES = "messages";
    private static final String TEST_COMPLETE = "Test complete!";
    private static final String COMMAND = "java -jar ./target/ASD-1.0-jar-with-dependencies.jar Contact=127.0.0.1:%d listen_base_port=%d";
    private static final String AUTOMATED_CLIENT_OUTPUT_FILE = "./automatedClientOutput/%d.txt";
    private static final String ERROR_READING_FILE_MSG = "Error reading file: %s";
    private static final String DEFAULT_TOPIC = "all";

    // Variables
    private static List<PrintWriter> clients;
    private static List<Process> processes;
    private static int nodes;
    private static int nodesToPublish;
    private static int numberTopics;
    private static int numberOfMessages;
    private static List<Integer> ports;
    private static List<String> topics;
    private static int killPeriod, killInit;
    private static int publishes;

    public static void main(String[] args) throws Exception {
        Babel.getInstance().loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        Properties properties = Babel.getInstance().loadConfig(AUTOMATED_CLIENT_CONFIG_PROPERTIES, args);
        nodes = PropertiesUtils.getPropertyAsInt(properties,NODES);
        clients = new ArrayList<>(nodes);
        ports = new ArrayList<>(nodes);
        numberTopics = PropertiesUtils.getPropertyAsInt(properties,TOPICS);
        topics = new ArrayList<>(numberTopics);
        processes = new ArrayList<>(nodes);
        killInit = PropertiesUtils.getPropertyAsInt(properties,KILL_INIT);
        killPeriod = PropertiesUtils.getPropertyAsInt(properties,KILL_PERIOD);
        numberOfMessages = PropertiesUtils.getPropertyAsInt(properties,MESSAGES);
        nodesToPublish = PropertiesUtils.getPropertyAsInt(properties,NODES_TO_PUBLISH);
        publishes = 0;

        initializeNodes();
        initializeTopics();

        Thread.sleep(1000);

        try {
            generateSubscribes();
            Thread.sleep(5000);
            generatePublishes(10);
        } finally {
            killNodes();
            System.out.println(TEST_COMPLETE);
            System.exit(1);
        }

    }

    public static void publisherAuto(PrintWriter c, String topic, String message) {
        c.printf(PUBLISH_CMD, topic, message, System.currentTimeMillis());
    }

    public static void subscriberAuto(PrintWriter c, String topic) {
        c.printf(SUBSCRIBE_CMD, topic);
    }

    public static void unsubscribeAuto(PrintWriter c, String topic) {
        c.printf(UNSUBSCRIBE_CMD, topic);
    }

    public static void initializeNodes() throws Exception {
        Random r = new Random(System.currentTimeMillis());
        int basePort = 10000;
        int port = basePort + 1;
        PrintWriter output = createNode(port, basePort);
        ports.add(basePort);
        clients.add(output);
        for (int i = 1; i < nodes; i++) {
            port = basePort + i;
            ports.add(port);

            output = createNode(basePort, port);
            clients.add(output);
            Thread.sleep(1000);
        }

    }

    public static void initializeTopics() {
        String baseTopic = "topic";
        for (int i = 0; i < numberTopics; i++) {
            topics.add(baseTopic + i);
        }
    }

    public static PrintWriter createNode(final int contact, final int port) throws IOException, InterruptedException {

        Process process = Runtime.getRuntime()
                .exec(String.format(COMMAND, contact, port));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(process.getOutputStream()), true);

        processes.add(process);

        Thread t = new Thread(() -> {
            File f = new File(String.format(AUTOMATED_CLIENT_OUTPUT_FILE, port));
            if (f.exists()) {
                f.delete();
            }

            try {
                f.createNewFile();
                FileOutputStream fOut = new FileOutputStream(f);
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                PrintWriter outPw = new PrintWriter(new OutputStreamWriter(fOut), true);
                while (true) {
                    String line = reader.readLine();
                    if (line != null) {
                        outPw.println(line);
                    }
                }
            } catch (FileNotFoundException e) {
                // Ignored - should not happen.
            } catch (IOException e) {
                System.err.println(String.format(ERROR_READING_FILE_MSG, f.toPath()));
            }
        });
        t.start();
        return out;
    }

    public static void killNodes() throws InterruptedException {
        Thread.sleep(killInit);
        for (Process p : processes) {
            p.destroy();
            Thread.sleep(killPeriod);
        }
    }

    public static void generateSubscribes() {
        for (PrintWriter c : clients) {
            subscriberAuto(c, DEFAULT_TOPIC);
        }
    }

    public static void generatePublishes(int nodesToPublish) throws InterruptedException {
        Random r = new Random(System.currentTimeMillis());
        String baseMessage = "message";
        for (int i = 0; i < nodesToPublish; i++) {
            PrintWriter c = clients.get(r.nextInt(clients.size()));
            for (int j = 0; j < numberOfMessages; j++) {
                publisherAuto(c, DEFAULT_TOPIC, baseMessage + (publishes++));
                Thread.sleep(50);
            }

        }
    }

}
