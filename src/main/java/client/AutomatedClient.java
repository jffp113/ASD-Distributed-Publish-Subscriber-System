package client;

import babel.Babel;
import utils.PropertiesUtils;

import java.io.*;
import java.util.*;

public class AutomatedClient {
    // Constants
    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";
    private static final String AUTOMATED_CLIENT_CONFIG_PROPERTIES = "src/automated_client.properties";
    private static final String NODES = "nodes";
    private static final String TOPICS = "topics";
    private static final String NODES_TO_PUBLISH = "nodes_to_publish";
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
    private static final String FAILURE_RATE = "failure_rate";
    private static final String FAILURE_TIMER = "failure_timer";
    private static final int basePort = 10000;

    // Variables
    private static Map<Integer, PrintWriter> clients;
    private static Map<Integer, Process> processes;
    private static int nodes, nodesToPublish, numberTopics, numberOfMessages, killPeriod, killInit, publishes, failureRate, failureTimer;
    private static List<Integer> ports;
    private static List<String> topics;

    public static void main(String[] args) throws Exception {
        Babel.getInstance().loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        Properties properties = Babel.getInstance().loadConfig(AUTOMATED_CLIENT_CONFIG_PROPERTIES, args);
        nodes = PropertiesUtils.getPropertyAsInt(properties, NODES);
        clients = new HashMap<>(nodes);
        ports = new ArrayList<>(nodes);
        numberTopics = PropertiesUtils.getPropertyAsInt(properties, TOPICS);
        topics = new ArrayList<>(numberTopics);
        processes = new HashMap<>(nodes);
        killInit = PropertiesUtils.getPropertyAsInt(properties, KILL_INIT);
        killPeriod = PropertiesUtils.getPropertyAsInt(properties, KILL_PERIOD);
        numberOfMessages = PropertiesUtils.getPropertyAsInt(properties, MESSAGES);
        nodesToPublish = PropertiesUtils.getPropertyAsInt(properties, NODES_TO_PUBLISH);
        failureRate = PropertiesUtils.getPropertyAsInt(properties, FAILURE_RATE);
        failureTimer = PropertiesUtils.getPropertyAsInt(properties, FAILURE_TIMER);
        publishes = 0;

        initializeNodes();
        initializeTopics();

        Thread.sleep(1000);

        try {
            generateSubscribes();
            Thread.sleep(5000);
            generatePublishes(nodesToPublish);
            Thread.sleep(15000);
            List<Integer> nodesCrashed = crashNodes();
            generatePublishes(nodes-nodesCrashed.size());
            for (Integer i : nodesCrashed) {
                PrintWriter out = createNode(basePort, i);
                clients.put(i, out);
            }
            Thread.sleep(2000);
            generatePublishes(nodesToPublish);
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
        int port = basePort + 1;
        // contacto porto
        PrintWriter output = createNode(port, basePort);
        ports.add(basePort);
        clients.put(basePort, output);
        for (int i = 1; i < nodes; i++) {
            port = basePort + i;
            ports.add(port);

            output = createNode(basePort, port);
            clients.put(basePort, output);
            Thread.sleep(1000);
        }

    }

    public static void initializeTopics() {
        String baseTopic = "topic";
        for (int i = 0; i < numberTopics; i++) {
            topics.add(baseTopic + i);
        }
    }

    public static PrintWriter createNode(final int contact, final int port) throws IOException {

        Process process = Runtime.getRuntime()
                .exec(String.format(COMMAND, contact, port));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(process.getOutputStream()), true);

        processes.put(port, process);

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
        for (Process p : processes.values()) {
            p.destroy();
            Thread.sleep(killPeriod);
        }
    }

    public static void generateSubscribes() {
        for (PrintWriter c : clients.values()) {
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

   private static List<Integer> crashNodes() throws InterruptedException {
        int nodesToFail = nodes * (failureRate / 100);
        List<Integer> nodesCrashed = new ArrayList<>(nodesToFail);
        Random r = new Random();
        for (int i = 0; i < nodesToFail; i++) {
            int index = r.nextInt(clients.size());
            int port = ports.get(index);
            PrintWriter out = clients.get(port);
            out.close();
            Process process = processes.get(port);
            process.destroy();
            nodesCrashed.add(port);
            Thread.sleep(failureTimer);
        }
        return nodesCrashed;
    }

}
