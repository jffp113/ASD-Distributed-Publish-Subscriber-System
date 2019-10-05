package client;

import babel.Babel;

import java.io.*;
import java.util.*;

public class AutomatedClient {

    private static final String NETWORK_CONFIG_PROPERTIES = "src/network_config.properties";
    private static final String AUTOMATED_CLIENT_CONFIG_PROPERTIES = "src/automated_client.properties";
    private static final String NODES = "nodes";
    private static final String TOPICS = "topics";
    private static final String KILL_INIT = "killInit";
    private static final String KILL_PERIOD = "killPeriod";
    private static final String PUBLISH_CMD = "publish %s %s time=%d\n";
    private static final String SUBSCRIBE_CMD = "subscribe %s\n";
    private static final String UNSUBSCRIBE_CMD = "unsubscribe %s";
    private static final String MESSAGES = "messages";
    private static List<PrintWriter> clients;
    private static List<Process> processes;
    private static int nodes;
    private static int numberTopics;
    private static int numberOfMessages;
    private static List<Integer> ports;
    private static List<String> topics;
    private static int killPeriod, killInit;
    private static int counter;

    public static void main(String[] args) throws Exception {
        Babel.getInstance().loadConfig(NETWORK_CONFIG_PROPERTIES, args);
        Properties properties = Babel.getInstance().loadConfig(AUTOMATED_CLIENT_CONFIG_PROPERTIES, args);
        nodes = Integer.parseInt(properties.getProperty(NODES, "10"));
        clients = new ArrayList<>(nodes);
        ports = new ArrayList<>(nodes);
        numberTopics = Integer.parseInt(properties.getProperty(TOPICS, "30"));
        topics = new ArrayList<>(numberTopics);
        processes = new ArrayList<>(nodes);
        killInit = Integer.parseInt(properties.getProperty(KILL_INIT, "4000"));
        killPeriod = Integer.parseInt(properties.getProperty(KILL_PERIOD, "400"));
        numberOfMessages=Integer.parseInt(properties.getProperty(MESSAGES,"10"));
        counter=0;
        initializeNodes();
        initializeTopics();

        Thread.sleep(1000);

        try{
            generateSubscribes();
            Thread.sleep(1000);
            generatePublishes(10);
        }finally {
            killNodes();
            System.out.println("DONEZOOOOOOOOOOOOO!");
            System.exit(1);
        }

    }

    public static void publisherAuto(PrintWriter c, String topic, String message) {
        c.printf(PUBLISH_CMD,  topic, message,System.currentTimeMillis());
    }

    public static void subscriberAuto(PrintWriter c, String topic) {
        c.printf(SUBSCRIBE_CMD, topic);
    }

    public static void unsubscribeAuto(PrintWriter c, String topic) {
        c.printf(UNSUBSCRIBE_CMD, topic);
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
            Thread.sleep(500);
        }

    }

    public static void initializeTopics() {
        for (int i = 0; i < numberTopics; i++) {
            topics.add("topic" + i);
        }
    }

    public static PrintWriter createNode(final int contact, final int port) throws IOException, InterruptedException {

        Process process = Runtime.getRuntime()
                .exec("java -jar ./target/ASD-1.0-jar-with-dependencies.jar Contact=127.0.0.1:" + contact + " listen_base_port=" + port);
        PrintWriter out = new PrintWriter(new OutputStreamWriter(process.getOutputStream()), true);

        processes.add(process);

        Thread t = new Thread(() -> {
            OutputStream outputStream = process.getOutputStream();
            File f = new File("./automatedClientOutput/" + port + ".txt");
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
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
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

    public static void generateSubscribes(){
        for(PrintWriter c : clients){
            subscriberAuto(c,"all");
        }
    }

    public static void generatePublishes(int nodesToPublish) throws InterruptedException {
        Random r = new Random(System.currentTimeMillis());
       for(int i=0; i<nodesToPublish;i++){
           PrintWriter c = clients.get(r.nextInt(clients.size()));
           for(int j=0;j<numberOfMessages;j++){
               publisherAuto(c,"all", "m"+(counter++));
               Thread.sleep(50);
           }

       }
    }
}
